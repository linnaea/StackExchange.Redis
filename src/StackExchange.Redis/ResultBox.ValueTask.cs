using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;

namespace StackExchange.Redis
{
    internal sealed partial class ValueTaskResultBox<T> : IResultBox<T>
    {
        private short _token;
        private ValueTaskSource _source;
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

        private ValueTaskResultBox() { }

        bool IResultBox.IsAsync => true;

        bool IResultBox.IsFaulted => _source.GetStatus(_token) switch
        {
            ValueTaskSourceStatus.Faulted => true,
            ValueTaskSourceStatus.Canceled => true,
            _ => false
        };

        void IResultBox.Cancel() => _source.TrySetException(_token, SimpleResultBox.CancelledException, false);

        void IResultBox.SetException(Exception ex) => _source.TrySetException(_token, ex ?? SimpleResultBox.CancelledException, false);

        void IResultBox<T>.SetResult(T value) => _source.TrySetResult(_token, value, false);

        T IResultBox<T>.GetResult(out Exception ex, bool canRecycle)
        {
            var r = _source.PeekResult(_token, out ex);
            if (canRecycle)
            {
                _source.CompleteTask(_token);
                BoxPool.Return(this);
            }

            return r;
        }

        public bool TrySetException(Exception ex) => _source.TrySetException(_token, ex ?? SimpleResultBox.CancelledException);

        void IResultBox.ActivateContinuations()
        {
            _source.CompleteTask(_token);
            BoxPool.Return(this);
        }

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

            task = BoxPool.Get();
            task._source = source;
            task._token = token;
            return task;
        }

        public static ValueTask<T> Default(object asyncState) => Completed(default, asyncState);
        public static ValueTask<T> Completed(T value, object asyncState)
        {
            if (asyncState == null) return new ValueTask<T>(value);
            ValueTaskSource.Allocate(asyncState, out var source, out var token);
            source.TrySetResult(token, value);
            return new ValueTask<T>(source, token);
        }

        private static class BoxPool
        {
            private const int TsStackSize = 5;
            private const int SharedPoolSize = 13;
            private static ValueTaskResultBox<T>[] TsStack => _tsStack ??= new ValueTaskResultBox<T>[TsStackSize];

            // ReSharper disable StaticMemberInGenericType
            [ThreadStatic] private static ValueTaskResultBox<T>[] _tsStack;
            [ThreadStatic] private static int _tsCount;
            private static readonly ValueTaskResultBox<T>[] sharedPool = new ValueTaskResultBox<T>[SharedPoolSize];
            // ReSharper restore StaticMemberInGenericType

            public static void Return(ValueTaskResultBox<T> v)
            {
                if (_tsCount < TsStackSize)
                {
                    TsStack[_tsCount++] = v;
                    return;
                }

                for (var i = 0; i < SharedPoolSize; i++)
                {
                    if (sharedPool[i] != null)
                        continue;

                    sharedPool[i] = v;
                    return;
                }
            }

            public static ValueTaskResultBox<T> Get()
            {
                var b = _tsCount > 0 ? TsStack[--_tsCount] : null;

                if (b == null)
                {
                    for (var i = 0; i < SharedPoolSize; i++)
                    {
                        var inst = sharedPool[i];
                        if (inst == null)
                            continue;

                        if (inst != Interlocked.CompareExchange(ref sharedPool[i], null, inst))
                            continue;

                        b = inst;
                        break;
                    }
                }

                if (b == null)
                    return new ValueTaskResultBox<T>();

                b._source = null;
                b._taskCreated = false;
                b._token = short.MinValue;
                return b;
            }
        }

        private partial class ValueTaskSource : IValueTaskSource<T>
        {
            private static ValueTaskSource _pool = new ValueTaskSource(13);
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
#if NET5_0_OR_GREATER
                Interlocked.Or(ref _status[token], ForceAsynchronousContinuation);
#else
                int s, next;
                do
                {
                    s = _status[token];
                    next = s | ForceAsynchronousContinuation;
                } while (Interlocked.CompareExchange(ref _status[token], next, s) != s);
#endif
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

                ExecutionContext execContext = null;
                _syncContext[token] = syncContext;
                if ((flags & ValueTaskSourceOnCompletedFlags.FlowExecutionContext) != 0)
                    _execContext[token] = execContext = ExecutionContext.Capture();

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

                ScheduleContinuation(continuation, state, syncContext, forceAsync, execContext);
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
                    if (!TransitionState(token, SlotStatus.Running, SlotStatus.Completed))
                        throw new InvalidOperationException();

                var continuation = Interlocked.Exchange(ref _continuation[token], CompletedValueTask.CompletionSentinel);
                var execContext = _execContext[token];
                var syncContext = _syncContext[token];
                var asyncState = _asyncState[token];
                var forceAsync = (_status[token] & ForceAsynchronousContinuation) == ForceAsynchronousContinuation;

                if (continuation == null || ReferenceEquals(continuation, CompletedValueTask.CompletionSentinel))
                    return;

                ScheduleContinuation(continuation, asyncState, syncContext, forceAsync, execContext);
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
