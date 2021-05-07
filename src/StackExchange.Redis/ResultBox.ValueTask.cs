using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace StackExchange.Redis
{
    internal sealed class ValueTaskResultBox<T> : IResultBox<T>
    {
        private readonly short _token;
        private readonly ValueTaskSource _source;
        private bool _taskCreated;

        public ValueTask<T> Task
        {
            get
            {
                if (_taskCreated)
                    throw new InvalidOperationException();

                _taskCreated = true;
                return new ValueTask<T>(_source, _token);
            }
        }

        private ValueTaskResultBox(ValueTaskSource source, short token)
        {
            _source = source;
            _token = token;
        }

        bool IResultBox.IsAsync => true;

        bool IResultBox.IsFaulted => _source.GetStatus(_token) == ValueTaskSourceStatus.Faulted;

        void IResultBox.Cancel() => _source.TrySetException(_token, SimpleResultBox.CancelledException, false);

        void IResultBox.SetException(Exception ex) => _source.TrySetException(_token, ex ?? SimpleResultBox.CancelledException, false);

        void IResultBox<T>.SetResult(T value) => _source.TrySetResult(_token, value, false);

        T IResultBox<T>.GetResult(out Exception ex, bool _) => _source.PeekResult(_token, out ex);

        public bool TrySetException(Exception ex) => _source.TrySetException(_token, ex ?? SimpleResultBox.CancelledException);

        void IResultBox.ActivateContinuations() => _source.CompleteTask(_token);

        public static IResultBox<T> Create(out ValueTaskResultBox<T> task, object asyncState)
        {
            // it might look a little odd to return the same object as two different things,
            // but that's because it is serving two purposes, and I want to make it clear
            // how it is being used in those 2 different ways; also, the *fact* that they
            // are the same underlying object is an implementation detail that the rest of
            // the code doesn't need to know about
            ValueTaskSource.Allocate(asyncState, out var source, out var token);
            if (ConnectionMultiplexer.PreventThreadTheft)
                source.MarkForceAsyncContinuation(token);

            return task = new ValueTaskResultBox<T>(source, token);
        }

        public static ValueTask<T> Default(object asyncState) => Completed(default, asyncState);
        public static ValueTask<T> Completed(T value, object asyncState)
        {
            if (asyncState == null) return new ValueTask<T>(value);
            ValueTaskSource.Allocate(asyncState, out var source, out var token);
            source.TrySetResult(token, value);
            return new ValueTask<T>(source, token);
        }

        private class ValueTaskSource : IValueTaskSource<T>
        {
            private static ValueTaskSource _pool = new ValueTaskSource(16);
            private const int ForceAsynchronousContinuation = 0x100;

            private readonly ExecutionContext[] _execContext;
            private readonly Action<object>[] _continuation;
            private readonly object[] _syncContext;
            private readonly object[] _asyncState;

            private readonly Exception[] _exception;
            private readonly int[] _status;
            private readonly T[] _result;

            private ValueTaskSource(ushort slots)
            {
                _execContext = new ExecutionContext[slots];
                _continuation = new Action<object>[slots];
                _syncContext = new object[slots];
                _asyncState = new object[slots];
                _exception = new Exception[slots];
                _status = new int[slots];
                _result = new T[slots];
            }

            private enum SlotStatus : byte
            {
                Free,
                Running,
                Completing,
                Completed,
                Cleanup
            }

            public void MarkForceAsyncContinuation(short token)
            {
                int s, next;
                do
                {
                    s = _status[token];
                    next = s | ForceAsynchronousContinuation;
                } while (Interlocked.CompareExchange(ref _status[token], next, s) != s);
            }

            public ValueTaskSourceStatus GetStatus(short token)
            {
                switch ((SlotStatus) unchecked((byte)_status[token]))
                {
                case SlotStatus.Running:
                case SlotStatus.Completing:
                    return ValueTaskSourceStatus.Pending;
                case SlotStatus.Completed:
                    return _exception[token] switch
                    {
                        null => ValueTaskSourceStatus.Succeeded,
                        OperationCanceledException _ => ValueTaskSourceStatus.Canceled,
                        _ => ValueTaskSourceStatus.Faulted
                    };
                case SlotStatus.Cleanup:
                case SlotStatus.Free:
                    throw new InvalidOperationException("Misuse of ValueTask");
                default:
                    throw new InvalidOperationException();
                }
            }

            public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
            {
                GetStatus(token);
                object syncContext = null;
                if ((flags & ValueTaskSourceOnCompletedFlags.UseSchedulingContext) != 0)
                {
                    var sc = SynchronizationContext.Current;
                    if (sc == null || sc.GetType() == typeof(SynchronizationContext))
                    {
                        var ts = TaskScheduler.Current;
                        if (ts != TaskScheduler.Default)
                        {
                            syncContext = ts;
                        }
                    }
                    else
                    {
                        syncContext = sc;
                    }
                }

                var forceAsync = (_status[token] & ForceAsynchronousContinuation) == ForceAsynchronousContinuation;
                if (ReferenceEquals(_continuation[token], CompletedValueTask.CompletionSentinel))
                {
                    ScheduleContinuation(continuation, state, syncContext, forceAsync,
                                         (flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0);
                    return;
                }

                _syncContext[token] = syncContext;
                if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                    _execContext[token] = ExecutionContext.Capture();

                var oldContinuation = _continuation[token];
                if (oldContinuation == null)
                {
                    _asyncState[token] = state;
                    oldContinuation = Interlocked.CompareExchange(ref _continuation[token], continuation, null);
                }

                if (oldContinuation == null)
                    return;

                if (!ReferenceEquals(oldContinuation, CompletedValueTask.CompletionSentinel))
                    throw new InvalidOperationException();

                ScheduleContinuation(continuation, state, syncContext, forceAsync,
                                     (flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0);
            }

            public T GetResult(short token)
            {
                var r = PeekResult(token, out var e);
                InvalidateSlot(token);
                if (e != null)
                    ExceptionDispatchInfo.Capture(e).Throw();
                return r;
            }

            public T PeekResult(short token, out Exception ex)
            {
                switch ((SlotStatus) unchecked((byte)_status[token]))
                {
                case SlotStatus.Running:
                    throw new InvalidOperationException("Synchronized waiting is not allowed");
                case SlotStatus.Completing:
                case SlotStatus.Completed:
                    ex = _exception[token];
                    return _result[token];
                case SlotStatus.Cleanup:
                case SlotStatus.Free:
                    throw new InvalidOperationException("Misuse of ValueTask");
                default:
                    throw new InvalidOperationException();
                }
            }

            public bool TrySetResult(short token, T result, bool completeTask = true)
                => TrySetCompletion(token, result, null, completeTask);

            public bool TrySetException(short token, Exception e, bool completeTask = true)
                => TrySetCompletion(token, default, e, completeTask);

            private bool TrySetCompletion(short token, T result, Exception e, bool completeTask)
            {
                if (!TransitionState(token, SlotStatus.Running, SlotStatus.Completing))
                    return false;

                _result[token] = result;
                _exception[token] = e;

                if (completeTask)
                    CompleteTask(token);

                return true;
            }

            public void CompleteTask(short token)
            {
                if (!TransitionState(token, SlotStatus.Completing, SlotStatus.Completed))
                    throw new InvalidOperationException();

                var continuation = Interlocked.Exchange(ref _continuation[token], CompletedValueTask.CompletionSentinel);
                var execContext = _execContext[token];
                var syncContext = _syncContext[token];
                var asyncState = _asyncState[token];
                var forceAsync = (_status[token] & ForceAsynchronousContinuation) == ForceAsynchronousContinuation;

                if (continuation == null || ReferenceEquals(continuation, CompletedValueTask.CompletionSentinel))
                    return;

                if (execContext != null)
                {
                    ExecutionContext.Run(execContext, ScheduleContinuationInEc,
                                         Tuple.Create(continuation, asyncState, syncContext, forceAsync));
                }
                else
                {
                    ScheduleContinuation(continuation, asyncState, syncContext, forceAsync, false);
                }
            }

            private static void ScheduleContinuation(Action<object> continuation, object asyncState,
                                                     object syncContext, bool forceAsync, bool keepEc)
            {
                var threadPoolQueue = keepEc
                                          ? new Func<WaitCallback, object, bool>(ThreadPool.QueueUserWorkItem)
                                          : ThreadPool.UnsafeQueueUserWorkItem;

                if (syncContext is null)
                    threadPoolQueue(new WaitCallback(continuation), asyncState);
                else if (forceAsync)
                    threadPoolQueue(InvokeContinuation, Tuple.Create(continuation, asyncState, syncContext));
                else
                    InvokeContinuation(continuation, asyncState, syncContext);
            }

            private static void ScheduleContinuationInEc(object icp)
            {
                var cp = (Tuple<Action<object>, object, object, bool>) icp;
                ScheduleContinuation(cp.Item1, cp.Item2, cp.Item3, cp.Item4, true);
            }

            private static void InvokeContinuation(object icp)
            {
                var cp = (Tuple<Action<object>, object, object>) icp;
                InvokeContinuation(cp.Item1, cp.Item2, cp.Item3);
            }

            private static void InvokeContinuation(Action<object> continuation, object asyncState, object syncContext)
            {
                switch (syncContext)
                {
                case TaskScheduler ts:
                    System.Threading.Tasks.Task.Factory.StartNew(continuation, asyncState, CancellationToken.None,
                                                                 TaskCreationOptions.DenyChildAttach, ts);
                    break;
                case SynchronizationContext sc:
                    sc.Post(continuationParameters =>
                    {
                        var cp = (Tuple<Action<object>, object>) continuationParameters;
                        cp.Item1(cp.Item2);
                    }, Tuple.Create(continuation, asyncState));
                    break;
                }
            }

            private void InvalidateSlot(short token)
            {
                if (!TransitionState(token, SlotStatus.Completed, SlotStatus.Cleanup))
                    throw new InvalidOperationException();

                _execContext[token] = null;
                _syncContext[token] = null;
                _asyncState[token] = null;
                _result[token] = default;
                _exception[token] = null;
                _continuation[token] = null;
                _status[token] = 0;
            }

            private bool TryTakeSlot(short token, object state)
            {
                if (!TransitionState(token, SlotStatus.Free, SlotStatus.Running))
                    return false;

                _asyncState[token] = state;
                return true;
            }

            private bool TransitionState(short token, SlotStatus from, SlotStatus to)
            {
                var s = _status[token];
                if ((s & 0xFF) != (byte) from) return false;
                var next = s & unchecked((int)0xFFFFFF00);
                next |= (byte) to;
                return Interlocked.CompareExchange(ref _status[token], next, s) == s;
            }

            public static void Allocate(object state, out ValueTaskSource ts, out short token)
            {
                while (true)
                {
                    ts = _pool;
                    for (token = 0; token < ts._result.Length; token++)
                        if (ts.TryTakeSlot(token, state))
                            return;

                    _pool = ts._result.Length > short.MaxValue / 2
                                ? new ValueTaskSource((ushort) ts._result.Length)
                                : new ValueTaskSource((ushort) (ts._result.Length * 2));
                }
            }
        }
    }

    internal static class CompletedValueTask
    {
        public static readonly Action<object> CompletionSentinel = _ => throw new InvalidOperationException();
    }
}
