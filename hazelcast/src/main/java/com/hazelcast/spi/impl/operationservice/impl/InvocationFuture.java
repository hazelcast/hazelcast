/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.NULL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.WAIT_RESPONSE;
import static com.hazelcast.util.ExceptionUtil.fixRemoteStackTrace;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.lang.Math.min;

/**
 * The InvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of a {@link Invocation}. The Invocation executes an operation.
 *
 * @param <E>
 */
final class InvocationFuture<E> implements InternalCompletableFuture<E> {

    private static final int MAX_CALL_TIMEOUT_EXTENSION = 60 * 1000;

    private static final AtomicReferenceFieldUpdater<InvocationFuture, Object> RESPONSE
            = AtomicReferenceFieldUpdater.newUpdater(InvocationFuture.class, Object.class, "response");
    private static final AtomicIntegerFieldUpdater<InvocationFuture> WAITER_COUNT
            = AtomicIntegerFieldUpdater.newUpdater(InvocationFuture.class, "waiterCount");

    volatile boolean interrupted;
    volatile Object response;

    // Contains the number of threads waiting for a result from this future.
    // is updated through the WAITER_COUNT.
    private volatile int waiterCount;
    private final OperationServiceImpl operationService;
    private final Invocation invocation;
    private volatile ExecutionCallbackNode<E> callbackHead;

    InvocationFuture(OperationServiceImpl operationService, Invocation invocation, Object callback) {
        this.invocation = invocation;
        this.operationService = operationService;

        if (callback != null) {
            ExecutionCallback executionCallback;
            if (callback instanceof ExecutionCallback) {
                executionCallback = (ExecutionCallback) callback;
            } else {
                executionCallback = new ExecutorCallbackAdapter<E>((Callback) callback);
            }

            callbackHead = new ExecutionCallbackNode<E>(executionCallback, operationService.asyncExecutor, null);
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
            if (responseAvailable(response)) {
                runAsynchronous(callback, executor);
                return;
            }

            this.callbackHead = new ExecutionCallbackNode<E>(callback, executor, callbackHead);
        }
    }

    private boolean responseAvailable(Object response) {
        if (response == null) {
            return false;
        }

        if (response == InternalResponse.WAIT_RESPONSE) {
            return false;
        }

        return true;
    }

    @Override
    public void andThen(ExecutionCallback<E> callback) {
        andThen(callback, operationService.asyncExecutor);
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
                        invocation.logger.severe("Failed asynchronous execution of execution callback: " + callback
                                + "for call " + invocation, cause);
                    }
                }
            });
        } catch (RejectedExecutionException e) {
            invocation.logger.warning("Execution of callback: " + callback + " is rejected!", e);
        }
    }

    /**
     * Can be called multiple times, but only the first answer will lead to the future getting triggered. All subsequent
     * 'set' calls are ignored.
     *
     * @param offeredResponse The type of response to offer.
     */
    public void set(Object offeredResponse) {
        assert !(offeredResponse instanceof Response) : "unexpected response found: " + offeredResponse;

        if (offeredResponse == null) {
            offeredResponse = NULL_RESPONSE;
        }

        ExecutionCallbackNode<E> callbackChain;
        synchronized (this) {
            if (response != null && !(response instanceof InternalResponse)) {
                //it can be that this invocation future already received an answer, e.g. when an invocation
                //already received a response, but before it cleans up itself, it receives a
                //HazelcastInstanceNotActiveException.

                // this is no good; no logging while holding a lock
                ILogger logger = invocation.logger;
                if (logger.isFinestEnabled()) {
                    logger.finest("Future response is already set! Current response: "
                            + response + ", Offered response: " + offeredResponse + ", Invocation: " + invocation);
                }
                return;
            }

            response = offeredResponse;
            if (offeredResponse == WAIT_RESPONSE) {
                return;
            }
            callbackChain = callbackHead;
            callbackHead = null;
            notifyAll();

            operationService.invocationsRegistry.deregister(invocation);
        }


        notifyCallbacks(callbackChain);
    }

    private void notifyCallbacks(ExecutionCallbackNode<E> callbackChain) {
        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            invocation.logger.severe("Unexpected timeout while processing " + this, e);
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
        if (responseAvailable(response)) {
            return response;
        }

        WAITER_COUNT.incrementAndGet(this);
        try {
            long timeoutMs = toTimeoutMs(time, unit);
            long maxCallTimeoutMs = getMaxCallTimeout();
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

                    if (response == WAIT_RESPONSE) {
                        RESPONSE.compareAndSet(this, WAIT_RESPONSE, null);
                        continue;
                    } else if (response != null) {
                        //if the thread is interrupted, but the response was not an interrupted-response,
                        //we need to restore the interrupt flag.
                        if (response != INTERRUPTED_RESPONSE && interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return response;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }

                if (!interrupted && longPolling) {
                    // no response!
                    Address target = invocation.getTarget();
                    if (invocation.remote && invocation.nodeEngine.getThisAddress().equals(target)) {
                        // target may change during invocation because of migration!
                        continue;
                    }

                    invocation.logger.warning("No response for " + lastPollTime + " ms. " + toString());
                    boolean executing = operationService.getIsStillRunningService().isOperationExecuting(invocation);
                    if (!executing) {
                        Object operationTimeoutException = invocation.newOperationTimeoutException(pollCount * pollTimeoutMs);
                        // tries to set an OperationTimeoutException response if response is not set yet
                        set(operationTimeoutException);
                    }
                }
            }
            return TIMEOUT_RESPONSE;
        } finally {
            WAITER_COUNT.decrementAndGet(this);
        }
    }

    private void pollResponse(long pollTimeoutMs) throws InterruptedException {
        //we should only wait if there is any timeout. We can't call wait with 0, because it is interpreted as infinite.
        if (pollTimeoutMs <= 0 || response != null) {
            return;
        }

        long currentTimeoutMs = pollTimeoutMs;
        long waitStart = Clock.currentTimeMillis();
        synchronized (this) {
            while (currentTimeoutMs > 0 && response == null) {
                wait(currentTimeoutMs);
                currentTimeoutMs = pollTimeoutMs - (Clock.currentTimeMillis() - waitStart);
            }
        }
    }

    long getMaxCallTimeout() {
        long callTimeout = invocation.callTimeout;
        long maxCallTimeout = callTimeout + getCallTimeoutExtension(callTimeout);
        return maxCallTimeout > 0 ? maxCallTimeout : Long.MAX_VALUE;
    }

    private static long getCallTimeoutExtension(long callTimeout) {
        if (callTimeout <= 0) {
            return 0L;
        }

        return Math.min(callTimeout, MAX_CALL_TIMEOUT_EXTENSION);
    }

    int getWaitingThreadsCount() {
        return waiterCount;
    }

    private static long toTimeoutMs(long time, TimeUnit unit) {
        long timeoutMs = unit.toMillis(time);
        if (timeoutMs < 0) {
            timeoutMs = 0;
        }
        return timeoutMs;
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
        if (unresolvedResponse == NULL_RESPONSE) {
            return null;
        }

        if (unresolvedResponse == TIMEOUT_RESPONSE) {
            return new TimeoutException("Call " + invocation + " encountered a timeout");
        }

        if (unresolvedResponse == INTERRUPTED_RESPONSE) {
            return new InterruptedException("Call " + invocation + " was interrupted");
        }

        Object response = unresolvedResponse;
        if (invocation.resultDeserialized && response instanceof Data) {
            response = invocation.nodeEngine.toObject(response);
            if (response == null) {
                return null;
            }
        }

        if (response instanceof Throwable) {
            Throwable throwable = ((Throwable) response);
            if (invocation.remote) {
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
         return responseAvailable(response);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("InvocationFuture{");
        sb.append("invocation=").append(invocation.toString());
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
