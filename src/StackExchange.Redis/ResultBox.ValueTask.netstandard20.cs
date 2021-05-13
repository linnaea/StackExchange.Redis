#if !(NETCOREAPP2_1_OR_GREATER || NETSTANDARD2_1)
using System;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis
{
    internal sealed partial class ValueTaskResultBox<T>
    {
        private partial class ValueTaskSource
        {
            private static void ScheduleContinuation(Action<object> continuation, object asyncState,
                                                     object syncContext, bool forceAsync, ExecutionContext execContext)
            {
                if (syncContext == null)
                {
                    if (execContext == null)
                    {
                        ThreadPool.UnsafeQueueUserWorkItem(new WaitCallback(continuation), asyncState);
                    }
                    else
                    {
                        ThreadPool.UnsafeQueueUserWorkItem(ExecuteContinuationInContext,
                                                           Tuple.Create(execContext, continuation, asyncState));
                    }
                }
                else
                {
                    if (forceAsync)
                    {
                        ThreadPool.UnsafeQueueUserWorkItem(InvokeContinuation,
                                                           Tuple.Create(continuation, asyncState, syncContext, execContext));
                    }
                    else
                    {
                        InvokeContinuation(continuation, asyncState, syncContext, execContext);
                    }
                }
            }

            private static void ExecuteContinuationInContext(object icp)
            {
                var cp = (Tuple<ExecutionContext, Action<object>, object>)icp;
                ExecutionContext.Run(cp.Item1, new ContextCallback(cp.Item2), cp.Item3);
            }

            private static void InvokeContinuation(object icp)
            {
                var cp = (Tuple<Action<object>, object, object, ExecutionContext>) icp;
                InvokeContinuation(cp.Item1, cp.Item2, cp.Item3, cp.Item4);
            }

            private static void InvokeContinuation(Action<object> continuation, object asyncState, object syncContext, ExecutionContext execContext)
            {
                switch (syncContext)
                {
                case TaskScheduler ts:
                    if (execContext != null)
                    {
                        System.Threading.Tasks.Task.Factory.StartNew(ExecuteContinuationInContext,
                                                                     Tuple.Create(execContext, continuation, asyncState),
                                                                     CancellationToken.None,
                                                                     TaskCreationOptions.DenyChildAttach, ts);
                    }
                    else
                    {
                        System.Threading.Tasks.Task.Factory.StartNew(continuation, asyncState, CancellationToken.None,
                                                                     TaskCreationOptions.DenyChildAttach, ts);
                    }
                    break;
                case SynchronizationContext sc:
                    if (execContext != null)
                    {
                        sc.Post(ExecuteContinuationInContext, Tuple.Create(execContext, continuation, asyncState));
                    }
                    else
                    {
                        sc.Post(new SendOrPostCallback(continuation), asyncState);
                    }
                    break;
                }
            }
        }
    }
}
#endif
