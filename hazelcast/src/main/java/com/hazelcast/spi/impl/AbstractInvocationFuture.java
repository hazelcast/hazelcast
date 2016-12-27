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

package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.InternalCompletableFuture;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;
import static java.util.concurrent.locks.LockSupport.park;
import static java.util.concurrent.locks.LockSupport.parkNanos;
import static java.util.concurrent.locks.LockSupport.unpark;

/**
 * todo:
 * - thread value protection
 *
 * The long term goal is that this whole class can be ripped out and replaced by the
 * {@link java.util.concurrent.java.util.concurrent.CompletableFuture}. So we need to be very careful with adding more
 * functionality to this class because the more we add, the more difficult the replacement will be.
 *
 * @param <V>
 */
@SuppressFBWarnings(value = "DLS_DEAD_STORE_OF_CLASS_LITERAL", justification = "Recommended way to prevent classloading bug")
public abstract class AbstractInvocationFuture<V> implements InternalCompletableFuture<V> {

    static final Object VOID = "VOID";

    // Reduce the risk of rare disastrous classloading in first call to
    // LockSupport.park: https://bugs.openjdk.java.net/browse/JDK-8074773
    static {
        Class<?> ensureLoaded = LockSupport.class;
    }

    private static final AtomicReferenceFieldUpdater<AbstractInvocationFuture, Object> STATE =
            newUpdater(AbstractInvocationFuture.class, Object.class, "state");

    protected final Executor defaultExecutor;
    protected final ILogger logger;

    /**
     * This field contain the state of the future. Either the future is not complete and the state is:
     * <ol>
     * <li>VOID: no response is available.</li>
     * <li>Thread instance: no response is available and a thread has blocked on completion (e.g. future.get)</li>
     * <li>{@link ExecutionCallback} instance: no response is available and 1 {@link #andThen(ExecutionCallback)}
     * was done using the default executor</li>
     * <li>{@link WaitNode} instance: in case of multiple andThen registrations or future.gets or andThen with custom Executor
     * </li>
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
    private volatile Object state = VOID;

    public AbstractInvocationFuture(Executor defaultExecutor, ILogger logger) {
        this.defaultExecutor = defaultExecutor;
        this.logger = logger;
    }

    boolean compareAndSetState(Object oldState, Object newState) {
        return STATE.compareAndSet(this, oldState, newState);
    }

    protected final Object getState() {
        return state;
    }

    @Override
    public final boolean isDone() {
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

    protected void onInterruptDetected() {
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public final boolean isCancelled() {
        return false;
    }

    @Override
    public final V join() {
        try {
            // this method is quite inefficient when there is unchecked exception, because it will be wrapped
            // in a ExecutionException, and then it is unwrapped again.
            return get();
        } catch (Throwable throwable) {
            throw rethrow(throwable);
        }
    }

    @Override
    public final V getSafely() {
        return join();
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != VOID) {
            // no registration was done since a value is available.
            return resolveAndThrow(response);
        }

        boolean interrupted = false;
        try {
            for (; ; ) {
                park();
                if (isDone()) {
                    return resolveAndThrow(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }
    }

    @Override
    public final V get(final long timeout, final TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        Object response = registerWaiter(Thread.currentThread(), null);
        if (response != VOID) {
            return resolveAndThrow(response);
        }

        long deadlineNanos = System.nanoTime() + unit.toNanos(timeout);
        boolean interrupted = false;
        try {
            long timeoutNanos = unit.toNanos(timeout);
            while (timeoutNanos > 0) {
                parkNanos(timeoutNanos);
                timeoutNanos = deadlineNanos - System.nanoTime();

                if (isDone()) {
                    return resolveAndThrow(state);
                } else if (Thread.interrupted()) {
                    interrupted = true;
                    onInterruptDetected();
                }
            }
        } finally {
            restoreInterrupt(interrupted);
        }

        unregisterWaiter(Thread.currentThread());
        throw newTimeoutException(timeout, unit);
    }

    private void restoreInterrupt(boolean interrupted) {
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public final void andThen(ExecutionCallback<V> callback) {
        andThen(callback, defaultExecutor);
    }

    @Override
    public final void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        Object response = registerWaiter(callback, executor);
        if (response != VOID) {
            unblock(callback, executor);
        }
    }

    private void unblockAll(Object waiter, Executor executor) {
        while (waiter != null) {
            if (waiter instanceof Thread) {
                unpark((Thread) waiter);
                return;
            } else if (waiter instanceof ExecutionCallback) {
                unblock((ExecutionCallback) waiter, executor);
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

    private void unblock(final ExecutionCallback<V> callback, Executor executor) {
        try {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object value = resolve(state);
                        if (value instanceof Throwable) {
                            Throwable error = unwrap((Throwable) value);
                            callback.onFailure(error);
                        } else {
                            callback.onResponse((V) value);
                        }
                    } catch (Throwable cause) {
                        logger.severe("Failed asynchronous execution of execution callback: " + callback
                                + "for call " + invocationToString(), cause);
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            callback.onFailure(e);
        }
    }

    // this method should not be needed; but there is a difference between client and server how it handles async throwables.
    protected Throwable unwrap(Throwable throwable) {
        if (throwable instanceof ExecutionException) {
            return throwable.getCause();
        }
        return throwable;
    }

    protected abstract String invocationToString();

    protected Object resolve(Object value) {
        return value;
    }

    protected abstract V resolveAndThrow(Object state) throws ExecutionException, InterruptedException;

    protected abstract TimeoutException newTimeoutException(long timeout, TimeUnit unit);

    /**
     * Registers a waiter (thread/ExecutionCallback) that gets notified when the future completes.
     *
     * @param waiter   the waiter
     * @param executor the {@link Executor} to use in case of an {@link ExecutionCallback}.
     * @return VOID if the registration was a success, anything else but void is the response.
     */
    private Object registerWaiter(Object waiter, Executor executor) {
        WaitNode waitNode = null;
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                return oldState;
            }

            Object newState;
            if (oldState == VOID && (executor == null || executor == defaultExecutor)) {
                // Nothing is syncing on this future, so instead of creating a WaitNode, we just try to cas the waiter
                newState = waiter;
            } else {
                // something already has been registered for syncing. So we need to create a WaitNode.
                if (waitNode == null) {
                    waitNode = new WaitNode(waiter, executor);
                }
                waitNode.next = oldState;
                newState = waitNode;
            }

            if (compareAndSetState(oldState, newState)) {
                // we have successfully registered
                return VOID;
            }
        }
    }

    void unregisterWaiter(Thread waiter) {
        WaitNode prev = null;
        Object current = state;

        while (current != null) {
            Object currentWaiter = current.getClass() == WaitNode.class ? ((WaitNode) current).waiter : current;
            Object next = current.getClass() == WaitNode.class ? ((WaitNode) current).next : null;

            if (currentWaiter == waiter) {
                // it is the item we are looking for; so lets try to remove it,
                if (prev == null) {
                    // it is the first item of the stack; so we need to change the head to the next.
                    Object n = next == null ? VOID : next;
                    // if we manage to cas, we are done, else we need to restart.
                    current = compareAndSetState(current, n) ? null : state;
                } else {
                    // remove the current item; this is done by letting the prev.next point to the next instead of current.
                    prev.next = next;
                    // end the loop
                    current = null;
                }
            } else {
                // it isn't the item we are looking for; so lets move on to the next.
                prev = current.getClass() == WaitNode.class ? (WaitNode) current : null;
                current = next;
            }
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
    public final boolean complete(Object value) {
        for (; ; ) {
            final Object oldState = state;
            if (isDone(oldState)) {
                if (oldState != value) {
                    // it can be that this future already completed, e.g. when an invocation already
                    // received a response, but before it cleans up itself, it receives a HazelcastInstanceNotActiveException.
                    logger.warning("Future.complete(Object value) can only be called once. Request: " + invocationToString()
                            + ", current value: " + oldState + ", offered value: " + value);
                }
                return false;
            }

            if (compareAndSetState(oldState, value)) {
                unblockAll(oldState, defaultExecutor);
                return true;
            }
        }
    }

    @Override
    public String toString() {
        Object state = getState();
        if (isDone(state)) {
            return "InvocationFuture{invocation=" + invocationToString() + ", value=" + state + '}';
        } else {
            return "InvocationFuture{invocation=" + invocationToString() + ", done=false}";
        }
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
    static final class WaitNode {
        final Object waiter;
        volatile Object next;
        private final Executor executor;

        WaitNode(Object waiter, Executor executor) {
            this.waiter = waiter;
            this.executor = executor;
        }
    }
}
