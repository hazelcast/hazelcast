package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.TraceableOperation;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.fixRemoteStackTrace;
import static com.hazelcast.util.ValidationUtil.isNotNull;
import static java.lang.Math.min;

/**
 * The BasicInvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of a {@link com.hazelcast.spi.impl.BasicInvocation}. The BasicInvocation executes an operation.
 *
 * @param <E>
 */
final class BasicInvocationFuture<E> implements InternalCompletableFuture<E> {

    private static final int CALL_TIMEOUT = 5000;

    private static final AtomicReferenceFieldUpdater<BasicInvocationFuture, Object> RESPONSE_FIELD_UPDATER
            = AtomicReferenceFieldUpdater.newUpdater(BasicInvocationFuture.class, Object.class, "response");

    volatile boolean interrupted;
    private BasicInvocation basicInvocation;
    private volatile ExecutionCallbackNode<E> callbackHead;
    private volatile Object response;
    private final BasicOperationService operationService;

    BasicInvocationFuture(BasicOperationService operationService, BasicInvocation basicInvocation, final Callback<E> callback) {
        this.basicInvocation = basicInvocation;
        this.operationService = operationService;

        if (callback != null) {
            ExecutorCallbackAdapter<E> adapter = new ExecutorCallbackAdapter<E>(callback);
            callbackHead = new ExecutionCallbackNode<E>(adapter, basicInvocation.getAsyncExecutor(), null);
        }
    }

    static long decrementTimeout(long timeout, long diff) {
        if (timeout == Long.MAX_VALUE) {
            return timeout;
        }

        return timeout - diff;
    }

    @Override
    public void andThen(ExecutionCallback<E> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        synchronized (this) {
            if (response != null) {
                runAsynchronous(callback, executor);
                return;
            }

            this.callbackHead = new ExecutionCallbackNode<E>(callback, executor, callbackHead);
        }
    }

    @Override
    public void andThen(ExecutionCallback<E> callback) {
        andThen(callback, basicInvocation.getAsyncExecutor());
    }

    private void runAsynchronous(final ExecutionCallback<E> callback, Executor executor) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object resp = resolveApplicationResponse(response);

                        if (resp == null || !(resp instanceof Throwable)) {
                            callback.onResponse((E) resp);
                        } else {
                            callback.onFailure((Throwable) resp);
                        }
                    } catch (Throwable cause) {
                        basicInvocation.logger.severe("Failed asynchronous execution of execution callback: " + callback
                                + "for call " + basicInvocation, cause);
                    }
                }
            });
        } catch (RejectedExecutionException ignore) {
            basicInvocation.logger.finest(ignore);
        }
    }

    /**
     * Can be called multiple times, but only the first answer will lead to the future getting triggered. All subsequent
     * 'set' calls are ignored.
     *
     * @param offeredResponse
     */
    public void set(Object offeredResponse) {
        offeredResponse = resolveInternalResponse(offeredResponse);

        ExecutionCallbackNode<E> callbackChain;
        synchronized (this) {
            if (response != null && !(response instanceof BasicInvocation.InternalResponse)) {
                //it can be that this invocation future already received an answer, e.g. when a an invocation
                //already received a response, but before it cleans up itself, it receives a
                //HazelcastInstanceNotActiveException.

                ILogger logger = basicInvocation.logger;
                if (logger.isFinestEnabled()) {
                    logger.info("Future response is already set! Current response: "
                            + response + ", Offered response: " + offeredResponse + ", Invocation: " + basicInvocation);
                }
                return;
            }

            response = offeredResponse;
            if (offeredResponse == BasicInvocation.WAIT_RESPONSE) {
                return;
            }
            callbackChain = callbackHead;
            callbackHead = null;
            notifyAll();
        }

        operationService.deregisterInvocation(basicInvocation);
        notifyCallbacks(callbackChain);
    }

    private void notifyCallbacks(ExecutionCallbackNode<E> callbackChain) {
        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    private Object resolveInternalResponse(Object rawResponse) {
        if (rawResponse == null) {
            throw new IllegalArgumentException("response can't be null: " + basicInvocation);
        }

        if (rawResponse instanceof NormalResponse) {
            rawResponse = ((NormalResponse) rawResponse).getValue();
        }

        if (rawResponse == null) {
            rawResponse = BasicInvocation.NULL_RESPONSE;
        }
        return rawResponse;
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            basicInvocation.logger.severe("Unexpected timeout while processing " + this, e);
            return null;
        }
    }

    @Override
    public E getSafely() {
        try {
            //this method is quite inefficient when there is unchecked exception, because it will be wrapped
            //in a ExecutionException, and then it is unwrapped again.
            return get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        Object unresolvedResponse = waitForResponse(timeout, unit);
        return (E) resolveApplicationResponseOrThrowException(unresolvedResponse);
    }

    private Object waitForResponse(long time, TimeUnit unit) {
        if (response != null && response != BasicInvocation.WAIT_RESPONSE) {
            return response;
        }

        long timeoutMs = getTimeoutMs(time, unit);
        long maxCallTimeoutMs = getMaxCallTimeoutMs();
        boolean longPolling = timeoutMs > maxCallTimeoutMs;

        int pollCount = 0;
        while (timeoutMs >= 0) {
            long pollTimeoutMs = min(maxCallTimeoutMs, timeoutMs);
            long startMs = Clock.currentTimeMillis();
            long lastPollTime = 0;
            pollCount++;

            try {
                pollResponse(pollTimeoutMs);
                lastPollTime = Clock.currentTimeMillis() - startMs;
                timeoutMs = decrementTimeout(timeoutMs, lastPollTime);

                if (response == BasicInvocation.WAIT_RESPONSE) {
                    RESPONSE_FIELD_UPDATER.compareAndSet(this, BasicInvocation.WAIT_RESPONSE, null);
                    continue;
                } else if (response != null) {
                    //if the thread is interrupted, but the response was not an interrupted-response,
                    //we need to restore the interrupt flag.
                    if (response != BasicInvocation.INTERRUPTED_RESPONSE && interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return response;
                }
            } catch (InterruptedException e) {
                interrupted = true;
            }

            if (!interrupted && longPolling) {
                // no response!
                Address target = basicInvocation.getTarget();
                if (basicInvocation.remote && basicInvocation.nodeEngine.getThisAddress().equals(target)) {
                    // target may change during invocation because of migration!
                    continue;
                }
                basicInvocation.logger.warning("No response for " + lastPollTime + " ms. " + toString());

                boolean executing = isOperationExecuting(target);
                if (!executing) {
                    Object operationTimeoutException = newOperationTimeoutException(pollCount, pollTimeoutMs);
                    // tries to set an OperationTimeoutException response if response is not set yet
                    set(operationTimeoutException);
                }
            }
        }
        return BasicInvocation.TIMEOUT_RESPONSE;
    }

    private void pollResponse(final long pollTimeoutMs) throws InterruptedException {
        //we should only wait if there is any timeout. We can't call wait with 0, because it is interpreted as infinite.
        if (pollTimeoutMs > 0 && response == null) {
            long currentTimeoutMs = pollTimeoutMs;
            final long waitStart = Clock.currentTimeMillis();
            synchronized (this) {
                while (currentTimeoutMs > 0 && response == null) {
                    wait(currentTimeoutMs);
                    currentTimeoutMs = pollTimeoutMs - (Clock.currentTimeMillis() - waitStart);
                }
            }
        }
    }

    private long getMaxCallTimeoutMs() {
        return basicInvocation.callTimeout > 0 ? basicInvocation.callTimeout * 2 : Long.MAX_VALUE;
    }

    private static long getTimeoutMs(long time, TimeUnit unit) {
        long timeoutMs = unit.toMillis(time);
        if (timeoutMs < 0) {
            timeoutMs = 0;
        }
        return timeoutMs;
    }

    private Object newOperationTimeoutException(int pollCount, long pollTimeoutMs) {
        boolean hasResponse = basicInvocation.potentialResponse != null;
        int backupsExpected = basicInvocation.backupsExpected;
        int backupsCompleted = basicInvocation.backupsCompleted;

        if (hasResponse) {
            return new OperationTimeoutException("No response for " + (pollTimeoutMs * pollCount) + " ms."
                    + " Aborting invocation! " + toString()
                    + " Not all backups have completed "
                    + " backups-expected:" + backupsExpected
                    + " backups-completed: " + backupsCompleted);
        } else {
            return new OperationTimeoutException("No response for " + (pollTimeoutMs * pollCount) + " ms."
                    + " Aborting invocation! " + toString()
                    + " No response has been send "
                    + " backups-expected:" + backupsExpected
                    + " backups-completed: " + backupsCompleted);
        }
    }

    private Object resolveApplicationResponseOrThrowException(Object unresolvedResponse)
            throws ExecutionException, InterruptedException, TimeoutException {

        Object response = resolveApplicationResponse(unresolvedResponse);

        if (response == null || !(response instanceof Throwable)) {
            return response;
        }

        if (response instanceof ExecutionException) {
            throw (ExecutionException) response;
        }

        if (response instanceof TimeoutException) {
            throw (TimeoutException) response;
        }

        if (response instanceof InterruptedException) {
            throw (InterruptedException) response;
        }

        if (response instanceof Error) {
            throw (Error) response;
        }

        // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
        throw new ExecutionException((Throwable) response);
    }

    private Object resolveApplicationResponse(Object unresolvedResponse) {
        if (unresolvedResponse == BasicInvocation.NULL_RESPONSE) {
            return null;
        }

        if (unresolvedResponse == BasicInvocation.TIMEOUT_RESPONSE) {
            return new TimeoutException("Call " + basicInvocation + " encountered a timeout");
        }

        if (unresolvedResponse == BasicInvocation.INTERRUPTED_RESPONSE) {
            return new InterruptedException("Call " + basicInvocation + " was interrupted");
        }

        Object response = unresolvedResponse;
        if (basicInvocation.resultDeserialized && response instanceof Data) {
            response = basicInvocation.nodeEngine.toObject(response);
            if (response == null) {
                return null;
            }
        }

        if (response instanceof NormalResponse) {
            NormalResponse responseObj = (NormalResponse) response;
            response = responseObj.getValue();

            if (response == null) {
                return null;
            }

            //it could be that the value of the response is Data.
            if (basicInvocation.resultDeserialized && response instanceof Data) {
                response = basicInvocation.nodeEngine.toObject(response);
                if (response == null) {
                    return null;
                }
            }
        }

        if (response instanceof Throwable) {
            Throwable throwable = ((Throwable) response);
            if (basicInvocation.remote) {
                fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
            }
            return throwable;
        }

        return response;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return response != null;
    }

    private boolean isOperationExecuting(Address target) {
        // ask if op is still being executed?
        Boolean executing = Boolean.FALSE;
        try {
            Operation isStillExecuting = createCheckOperation();

            BasicInvocation inv = new BasicTargetInvocation(
                    basicInvocation.nodeEngine, basicInvocation.serviceName, isStillExecuting,
                    target, 0, 0, CALL_TIMEOUT, null, null, true);
            Future f = inv.invoke();
            basicInvocation.logger.warning("Asking if operation execution has been started: " + toString());
            executing = (Boolean) basicInvocation.nodeEngine.toObject(f.get(CALL_TIMEOUT, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            basicInvocation.logger.warning("While asking 'is-executing': " + toString(), e);
        }
        basicInvocation.logger.warning("'is-executing': " + executing + " -> " + toString());
        return executing;
    }

    private Operation createCheckOperation() {
        if (basicInvocation.op instanceof TraceableOperation) {
            TraceableOperation traceable = (TraceableOperation) basicInvocation.op;
            return new TraceableIsStillExecutingOperation(basicInvocation.serviceName, traceable.getTraceIdentifier());
        } else {
            return new IsStillExecutingOperation(basicInvocation.op.getCallId());
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("BasicInvocationFuture{");
        sb.append("invocation=").append(basicInvocation.toString());
        sb.append(", response=").append(response);
        sb.append(", done=").append(isDone());
        sb.append('}');
        return sb.toString();
    }

    private static final class ExecutionCallbackNode<E> {
        private final ExecutionCallback<E> callback;
        private final Executor executor;
        private final ExecutionCallbackNode<E> next;

        private ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

    private static final class ExecutorCallbackAdapter<E> implements ExecutionCallback<E> {
        private final Callback callback;

        private ExecutorCallbackAdapter(Callback callback) {
            this.callback = callback;
        }

        @Override
        public void onResponse(E response) {
            callback.notify(response);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.notify(t);
        }
    }
}
