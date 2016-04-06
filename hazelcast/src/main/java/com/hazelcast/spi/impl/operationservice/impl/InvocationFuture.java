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
import com.hazelcast.util.Clock;

import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.VOID;
import static com.hazelcast.util.ExceptionUtil.fixAsyncStackTrace;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.lang.Thread.currentThread;

/**
 * The InvocationFuture is the {@link com.hazelcast.spi.InternalCompletableFuture} that waits on the completion
 * of a {@link Invocation}. The Invocation executes an operation.
 *
 * @param <E>
 */
final class InvocationFuture<E> implements InternalCompletableFuture<E> {

    private static final AtomicReferenceFieldUpdater<InvocationFuture, Object> STATE =
            AtomicReferenceFieldUpdater.newUpdater(InvocationFuture.class, Object.class, "state");

    volatile boolean interrupted;

    /**
     * This field contain the state of the future. Either the future is not complete and the state is:
     * <ol>
     * <li>VOID: no response is available.</li>
     * <li>Thread instance: no response is available and a thread has blocked on completion (e.g. future.get)</li>
     * <li>{@link ExecutionCallback} instance: no response is available and 1 {@link #andThen(ExecutionCallback, Executor)}
     * was done using the default executor</li>
     * <li>{@link WaitNode} instance: in case of multiple andThen registrations or future.gets</li>
     * </ol>
     *
     * If the state is anything else, it is completed.
     *
     * The reason why a single future.get or registered ExecutionCallback doesn't create a WaitNode is that we don't want to cause
     * additional litter since most of our API calls are a get or a single ExecutionCallback.
     *
     * The state field is replaced using a cas, so registration or setting a response is an atomic operation and therefor not
     * prone to data-races. There is no need to use synchronized blocks.
     */
    volatile Object state = VOID;

    final Invocation invocation;

    private final OperationServiceImpl operationService;
    private final boolean deserialize;

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

        Object response = registerWaiter(callback, executor);
        if (response != VOID) {
            unblockExecutionCallback(callback, executor);
        }
    }


    /**
     * Can be called multiple times, but only the first answer will lead to the future getting triggered. All subsequent
     * complete calls are ignored.
     *
     * @param value The type of response to offer.
     * @return <tt>true</tt> if offered response, either a final response or an internal response,
     * is set/applied, <tt>false</tt> otherwise. If <tt>false</tt> is returned, that means offered response is ignored
     * because a final response is already set to this future.
     */
    public boolean complete(Object value) {
        assert !(value instanceof Response) : "unexpected response found: " + value;

        for (; ; ) {
            Object oldState = state;
            if (isDone(oldState)) {
                // it can be that this invocation future already received an answer, e.g. when an invocation already received a
                // response, but before it cleans up itself, it receives a HazelcastInstanceNotActiveException.

                if (invocation.logger.isFinestEnabled()) {
                    invocation.logger.finest("Future response is already set! Current response: "
                            + state + ", Offered response: " + value + ", Invocation: " + invocation);
                }

                return false;
            }

            if (STATE.compareAndSet(this, oldState, value)) {
                operationService.invocationRegistry.deregister(invocation);
                unblockAll(oldState, operationService.asyncExecutor);
                return true;
            }
        }
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                LockSupport.unpark((Thread) waiter);
                return;
            } else if (waiter instanceof ExecutionCallback) {
                unblockExecutionCallback((ExecutionCallback) waiter, executor);
                return;
            } else if (waiter.getClass() == WaitNode.class) {
                WaitNode waitNode = (WaitNode) waiter;
                unblockAll(waitNode.waiter, waitNode.executor);
                waiter = waitNode.next;
            } else {
                return;
            }
        }
    }

    private void unblockExecutionCallback(final ExecutionCallback<E> callback, Executor executor) {
        //todo: hack to make sure executor is available
        if (executor == null) {
            executor = invocation.operationService.asyncExecutor;
        }

        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object resolvedResponse = resolve(state);

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
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        // todo: deal with interruption
        // we are going to park for a result; however it can be that we get spurious wake-ups so
        // we need to recheck the state. We don't need to reregister
        for (; ; ) {
            LockSupport.park();
            Object state = this.state;
            if (isDone(state)) {
                return resolveAndThrow(state);
            }
        }
    }


    @Override
    public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        long timeoutNanos = unit.toNanos(timeout);

        if (timeoutNanos <= 0) {
            Object response = state;
            if (isDone(response)) {
                return resolveAndThrow(response);
            } else {
                throw new TimeoutException(invocation.op.getClass().getSimpleName() + " failed to complete within "
                        + timeout + " " + unit + ". " + invocation);
            }
        }

        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        long startTimeNanos = System.nanoTime();
        // todo: deal with interruption
        // we are going to park for a result; however it can be that we get spurious wake-ups so
        // we need to recheck the state. We don't need to re-registerWaiter
        for (; ; ) {
            LockSupport.parkNanos(timeoutNanos);
            long endTimeNanos = System.nanoTime();
            timeoutNanos -= endTimeNanos - startTimeNanos;
            startTimeNanos = endTimeNanos;

            Object state = this.state;
            if (isDone(state)) {
                return resolveAndThrow(state);
            } else if (timeoutNanos <= 0) {
                throw new TimeoutException(invocation.op.getClass().getSimpleName() + " failed to complete within "
                        + timeout + " " + unit + ". " + invocation);
            }
        }
    }

    /**
     * Registers a waiter (so a Thread that blocks on the future completion, or a {@link ExecutionCallback} notified
     * a     *
     *
     * @param waiter   the waiter
     * @param executor the {@link Executor} to use in case of an {@link ExecutionCallback}.
     * @return VOID if the registration was a success, anything else but void is the response.
     */
    private Object registerWaiter(Object waiter, Executor executor) {
        WaitNode waitNode = null;
        for (; ; ) {
            Object oldState = state;
            if (isDone(oldState)) {
                return oldState;
            }

            Object newState;
            if (oldState == VOID && (executor == null || executor == operationService.asyncExecutor)) {
                // Nothing is syncing on this future, so instead of creating a WaitNode, we just try
                // the cas the thread (so no extra litter)
                newState = waiter;
            } else {
                // something already has been registered for syncing. So we need to create a WaitNode.
                if (waitNode == null) {
                    waitNode = new WaitNode(waiter, executor);
                }
                waitNode.next = oldState;
                newState = waitNode;
            }

            if (STATE.compareAndSet(this, oldState, newState)) {
                // we have successfully registered to be notified.
                return VOID;
            }
        }
    }

    private void restoreInterrupt(boolean threadInterrupted) {
        if (threadInterrupted && state != INTERRUPTED) {
            // if the thread got interrupted, but we did not manage to interrupt the invocation, we need to restore
            // the interrupt flag
            currentThread().interrupt();
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
        if (unresolved instanceof Data) {
            if (deserialize) {
                response = Response.deserializeValue(operationService.serializationService, (Data) response);
                if (response == null) {
                    return null;
                }
            } else {
                response = Response.getValueAsData(operationService.serializationService, (Data) response);
            }
        }

        if (response instanceof Throwable) {
            Throwable throwable = ((Throwable) response);
            fixAsyncStackTrace((Throwable) response, currentThread().getStackTrace());
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
                    .append(Clock.currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
            long lastHeartbeatMs = invocation.lastHeartbeatMillis;
            sb.append("Last heartbeat: ");
            if (lastHeartbeatMs == 0) {
                sb.append("never. ");
            } else {
                sb.append(new Date(lastHeartbeatMs)).append(". ");
            }
        } else {
            sb.append(invocation.op.getClass().getSimpleName())
                    .append(" got rejected before execution due to not starting within the operation-call-timeout of: ")
                    .append(invocation.callTimeoutMillis).append(" ms. ");

            sb.append("Total elapsed time: ")
                    .append(Clock.currentTimeMillis() - invocation.firstInvocationTimeMillis).append(" ms. ");
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
        return isDone(state);
    }

    private boolean isDone(final Object state) {
        if (state == null) {
            return true;
        }

        return !(state == VOID
                || state.getClass() == WaitNode.class
                || state instanceof Thread
                || state instanceof ExecutionCallback);
    }

    @Override
    public String toString() {
        return "InvocationFuture{invocation=" + invocation.toString() + ", response=" + state + ", done=" + isDone() + '}';
    }

    /**
     * Linked nodes to record waiting {@link Thread} or {@link ExecutionCallback} instances using a Treiber stack.
     *
     * A waiter is something that gets triggered when a response comes in. There are 2 types of waiters:
     * <ol>
     * <li>Thread: when a future.get is done.</li>
     * <li>ExecutionCallback: when a future.andThen is done</li>
     * </ol>
     * The waiter is either a Thread or an ExecutionCallback.
     *
     * The {@link WaitNode} is effectively immutable. Once the WaitNode is set in the 'state' field, it will not be modified.
     * Also updating the state, introduces a happens before relation so the 'next' field can be read safely.
     */
    private static final class WaitNode {
        private final Executor executor;
        private final Object waiter;
        private Object next;

        private WaitNode(Object waiter, Executor executor) {
            this.waiter = waiter;
            this.executor = executor;
        }
    }
}
