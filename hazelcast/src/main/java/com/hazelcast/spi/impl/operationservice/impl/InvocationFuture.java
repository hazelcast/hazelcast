/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.impl.operationservice.impl.responses.Response;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.VOID;
import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.isNotNull;
import static com.hazelcast.util.StringUtil.timeToString;
import static java.lang.System.currentTimeMillis;

/**
 * The InvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of a {@link Invocation}. The Invocation executes an operation.
 *
 * In the past the InvocationFuture.get logic was also responsible for detecting the heartbeat for blocking operations
 * using the CONTINUE_WAIT and detecting if an operation is still running using the IsStillRunning funtionality. This
 * has been removed from the future and moved into the {@link InvocationMonitor}.
 *
 * @param <E>
 */
final class InvocationFuture<E> implements InternalCompletableFuture<E> {

    volatile boolean interrupted;
    volatile Object response = VOID;

    final Invocation invocation;
    private final boolean deserialize;
    private final OperationServiceImpl operationService;
    private volatile ExecutionCallbackNode<E> callbackHead;

    InvocationFuture(OperationServiceImpl operationService, Invocation invocation, boolean deserialize) {
        this.invocation = invocation;
        this.operationService = operationService;
        this.deserialize = deserialize;
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
            if (response != VOID) {
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
                        Object resolvedResponse = resolve(response);

                        if (resolvedResponse == null || !(resolvedResponse instanceof Throwable)) {
                            callback.onResponse((E) resolvedResponse);
                        } else {
                            Throwable error = (Throwable) resolvedResponse;
                            if (error instanceof ExecutionException) {
                                error = error.getCause();
                            }
                            callback.onFailure(error);
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
    public boolean complete(Object offeredResponse) {
        assert !(offeredResponse instanceof Response) : "unexpected response found: " + offeredResponse;

        ExecutionCallbackNode<E> callbackChain;
        synchronized (this) {
            if (response != VOID) {
                // it can be that this invocation future already received an answer, e.g. when an invocation already received a
                // response, but before it cleans up itself, it receives a HazelcastInstanceNotActiveException.

                if (invocation.logger.isFinestEnabled()) {
                    invocation.logger.finest("Future response is already set! Current response: "
                            + response + ", Offered response: " + offeredResponse + ", Invocation: " + invocation);
                }

                return false;
            }

            response = offeredResponse;
            callbackChain = callbackHead;
            callbackHead = null;
            notifyAll();

            operationService.invocationRegistry.deregister(invocation);
        }

        notifyCallbacks(callbackChain);
        return true;
    }

    private void notifyCallbacks(ExecutionCallbackNode<E> callbackChain) {
        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    @Override
    public E join() {
        try {
            // this method is quite inefficient when there is unchecked exception, because it will be wrapped
            // in a ExecutionException, and then it is unwrapped again.
            return get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public E getSafely() {
        return join();
    }

    @Override
    public E get() throws InterruptedException, ExecutionException {
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        boolean threadInterrupted = false;
        try {
            synchronized (this) {
                while (response == VOID) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        threadInterrupted = true;
                        interrupted = true;
                    }
                }
            }

            return resolveAndThrow(response);
        } finally {
            restoreInterrupt(threadInterrupted);
        }
    }

    @Override
    public E get(final long timeout, final TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        boolean threadInterrupted = false;
        long timeoutMillis = unit.toMillis(timeout);
        final long deadLineMillis = currentTimeMillis() + timeoutMillis;
        try {
            synchronized (this) {
                while (response == VOID) {
                    if (timeoutMillis <= 0) {
                        throw new TimeoutException(invocation.op.getClass().getSimpleName() + " failed to complete within "
                                + timeout + " " + unit + ". " + invocation);
                    }

                    try {
                        wait(timeoutMillis);
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }

                    timeoutMillis = deadLineMillis - currentTimeMillis();
                }
            }

            return resolveAndThrow(response);
        } finally {
            restoreInterrupt(threadInterrupted);
        }
    }

    private void restoreInterrupt(boolean threadInterrupted) {
        if (threadInterrupted && response != INTERRUPTED) {
            // if the thread got interrupted, but we did not manage to interrupt the invocation, we need to restore
            // the interrupt flag
            Thread.currentThread().interrupt();
        }
    }

    private E resolveAndThrow(Object unresolved) throws ExecutionException, InterruptedException {
        Object response = resolve(unresolved);

        if (response == null || !(response instanceof Throwable)) {
            return (E) response;
        } else if (response instanceof ExecutionException) {
            throw (ExecutionException) response;
        } else if (response instanceof InterruptedException) {
            throw (InterruptedException) response;
        } else if (response instanceof Error) {
            throw (Error) response;
        } else {
            throw new ExecutionException((Throwable) response);
        }
    }

    private Object resolve(Object unresolved) {
        if (unresolved == null) {
            return null;
        } else if (unresolved == INTERRUPTED) {
            return new InterruptedException(invocation.op.getClass().getSimpleName() + " was interrupted. " + invocation);
        } else if (unresolved == CALL_TIMEOUT) {
            return newOperationTimeoutException(false);
        } else if (unresolved == HEARTBEAT_TIMEOUT) {
            return newOperationTimeoutException(true);
        }

        Object response = unresolved;
        if (deserialize && response instanceof Data) {
            response = invocation.nodeEngine.toObject(response);
            if (response == null) {
                return null;
            }
        }

        if (response instanceof Throwable) {
            Throwable throwable = ((Throwable) response);
            fixAsyncStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
            return throwable;
        }

        return response;
    }

    private Object newOperationTimeoutException(boolean heartbeatTimeout) {
        StringBuilder sb = new StringBuilder();
        if (heartbeatTimeout) {
            sb.append(invocation.op.getClass().getSimpleName())
                    .append(" invocation failed to complete due to operation-heartbeat-timeout. ");

            sb.append("Total elapsed time: ")
                    .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
            long lastHeartbeatMs = invocation.lastHeartbeatMillis;
            sb.append("Last heartbeat: ");
            if (lastHeartbeatMs == 0) {
                sb.append("never. ");
            } else {
                sb.append(timeToString(lastHeartbeatMs)).append(". ");
            }
        } else {
            sb.append(invocation.op.getClass().getSimpleName())
                    .append(" got rejected before execution due to not starting within the operation-call-timeout of: ")
                    .append(invocation.callTimeoutMillis).append(" ms. ");

            sb.append("Total elapsed time: ")
                    .append(currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
        }

        sb.append(invocation);
        String msg = sb.toString();
        return new ExecutionException(msg, new OperationTimeoutException(msg));
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
        return response != VOID;
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
