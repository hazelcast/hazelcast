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

import com.hazelcast.core.DeadOperationException;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.DEAD_OPERATION_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.NOTHING;
import static com.hazelcast.util.Clock.currentTimeMillis;
import static com.hazelcast.util.ExceptionUtil.fixRemoteStackTrace;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.lang.Thread.currentThread;

/**
 * The InvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of a {@link Invocation}. The Invocation executes an operation.
 *
 * In the new approach, no matter how long it takes for the call the get called, the invocation waits till the calltimeout
 * from the remote or till a lacking operation heartbeat.
 *
 *
 * Problem: wait long blocking operations.
 * By default the default call timeout is set on every operation, means that is should be executed in this timewindow,
 * or else fail. In the old approach an invocation would just be retried.
 * Currently we immediately run into a timeout exception which is not retried.
 * I think the meaning of call timeout is being abused here; the call should never timeout; at least not based on its
 * call timeout.
 *
 * @param <E>
 */
final class InvocationFuture<E> implements InternalCompletableFuture<E> {

    // Contains the value of the response. Once set to a not null value, it will never change.
    volatile Object response = NOTHING;
    final Invocation invocation;
    volatile ExecutionCallbackNode<E> callbackHead;

    private final OperationServiceImpl operationService;

    InvocationFuture(OperationServiceImpl operationService, Invocation invocation, ExecutionCallback callback) {
        this.invocation = invocation;
        this.operationService = operationService;

        if (callback != null) {
            callbackHead = new ExecutionCallbackNode<E>(callback, operationService.asyncExecutor, null);
        }
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
        return response != NOTHING;
    }

    @Override
    public void andThen(ExecutionCallback<E> callback) {
        andThen(callback, operationService.asyncExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<E> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        synchronized (this) {
            if (response != NOTHING) {
                runAsynchronous(callback, executor);
                return;
            }

            this.callbackHead = new ExecutionCallbackNode<E>(callback, executor, callbackHead);
        }
    }

    private void runAsynchronous(final ExecutionCallback<E> callback, Executor executor) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        try {
                            E resp = resolve(response);
                            callback.onResponse(resp);
                        } catch (Throwable t) {
                            if (t instanceof ExecutionException) {
                                t = t.getCause();
                            }
                            callback.onFailure(t);
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
     * @return <tt>true</tt> if offered response, either a final response or an internal response,
     * is set/applied, <tt>false</tt> otherwise. If <tt>false</tt> is returned, that means offered response is ignored
     * because a final response is already set to this future.
     */
    public void set(Object offeredResponse) {
        assert !(offeredResponse instanceof Response) : "unexpected response found: " + offeredResponse;

        ExecutionCallbackNode<E> callbackChain;
        synchronized (this) {
            if (response != NOTHING && !(response instanceof InternalResponse)) {
                //it can be that this invocation future already received an answer, e.g. when an invocation
                //already received a response, but before it cleans up itself, it receives a
                //HazelcastInstanceNotActiveException.

                // this is no good; no logging while holding a lock
                if (invocation.logger.isFinestEnabled()) {
                    invocation.logger.finest("Future response is already set! Current response: "
                            + response + ", Offered response: " + offeredResponse + ", Invocation: " + invocation);
                }

                operationService.invocationsRegistry.deregister(invocation);
                return;
            }

            response = offeredResponse;
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
    public E getSafely() {
        try {
            return get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        if (response != NOTHING) {
            return resolve(response);
        }

        boolean threadInterrupted = false;
        try {
            synchronized (this) {
                while (response == NOTHING) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        threadInterrupted = true;
                        invocation.signalInterrupt();
                    }
                }
            }

            return resolve(response);
        } finally {
            restoreInterruptIfNeeded(threadInterrupted);
        }
    }

    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (response != NOTHING) {
            return resolve(response);
        }

        boolean threadInterrupted = false;
        long deadLineMs = currentTimeMillis() + unit.toMillis(timeout);
        try {
            synchronized (this) {
                while (response == NOTHING) {
                    long remainingMs = deadLineMs - currentTimeMillis();
                    if (remainingMs <= 0) {
                        throw newTimeoutException(timeout, unit);
                    }

                    try {
                        wait(remainingMs);
                    } catch (InterruptedException e) {
                        threadInterrupted = true;
                        invocation.signalInterrupt();
                    }
                }
            }

            return resolve(response);
        } finally {
            restoreInterruptIfNeeded(threadInterrupted);
        }
    }

    private void restoreInterruptIfNeeded(boolean threadInterrupted) {
        if (threadInterrupted && response != INTERRUPTED_RESPONSE) {
            // if the thread got interrupted, but we did not manage to interrupt the invocation, we need to restore
            // the interrupt flag
            currentThread().interrupt();
        }
    }

    E resolve(Object response) throws ExecutionException, InterruptedException {
        if (response instanceof InternalResponse) {
            if (response == DEAD_OPERATION_RESPONSE) {
                throw newExecutionExceptionForDeadOperationResponse();
            } else if (response == CALL_TIMEOUT_RESPONSE) {
                throw newExecutionExceptionForCallTimeoutResponse();
            } else if (response == INTERRUPTED_RESPONSE) {
                throw newInterruptedException();
            } else {
                throw new IllegalArgumentException();
            }
        } else {
            if (response instanceof Data) {
                if (!invocation.deserialize) {
                    return (E) response;
                }

                response = invocation.nodeEngine.toObject(response);
            }

            if (response instanceof Throwable) {
                Throwable throwable = ((Throwable) response);
                fixRemoteStackTrace(throwable, currentThread().getStackTrace());
                // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
                throw new ExecutionException((Throwable) response);
            } else {
                return (E) response;
            }
        }
    }

    private TimeoutException newTimeoutException(long timeout, TimeUnit unit) {
        //todo: improve exception
        return new TimeoutException();
    }

    private InterruptedException newInterruptedException() {
        return new InterruptedException("Call " + invocation + " was interrupted");
    }

    private ExecutionException newExecutionExceptionForCallTimeoutResponse() {
        //todo:
        long totalTimeoutMs = 0;

        StringBuilder sb = new StringBuilder("No response for ").append(totalTimeoutMs).append(" ms")
                .append(" Aborting invocation! ").append(toString());

        if (invocation.pendingResponse != null) {
            sb.append(" Not all backups have completed! ");
        } else {
            sb.append(" No response has been received! ");
        }

        sb.append(" backups-expected:").append(invocation.backupsExpected)
                .append(" backups-completed: ").append(invocation.backupsCompleted);

        return new ExecutionException(new OperationTimeoutException(sb.toString()));
    }

    private ExecutionException newExecutionExceptionForDeadOperationResponse() {
        //todo:
        long totalTimeoutMs = 0;

        StringBuilder sb = new StringBuilder("No response for ").append(totalTimeoutMs).append(" ms")
                .append(" Aborting invocation! ").append(toString());

        if (invocation.pendingResponse != null) {
            sb.append(" Not all backups have completed! ");
        } else {
            sb.append(" No response has been received! ");
        }

        sb.append(" backups-expected:").append(invocation.backupsExpected)
                .append(" backups-completed: ").append(invocation.backupsCompleted);

        return new ExecutionException(new DeadOperationException(sb.toString()));
    }

    @Override
    public String toString() {
        return "InvocationFuture{invocation=" + invocation.toString() + ", response=" + response + ", done=" + isDone() + '}';
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
}
