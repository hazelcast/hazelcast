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

package com.hazelcast.spi.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.EmptyStatement;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.Preconditions.isNotNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

/**
 * A base {@link ICompletableFuture} implementation that may be explicitly completed by setting its
 * value through setResult.
 * Implements the logic of cancellation and callbacks execution.
 *
 * @param <V> The result type returned by this Future's {@code get} method
 */
@SuppressFBWarnings(value = "NN_NAKED_NOTIFY", justification = "State handled with CAS, naked notify correct")
public abstract class AbstractCompletableFuture<V> implements ICompletableFuture<V> {

    private static final Object INITIAL_STATE = new ExecutionCallbackNode(null, null, null);
    private static final Object CANCELLED_STATE = new Object();

    private static final AtomicReferenceFieldUpdater<AbstractCompletableFuture, Object> STATE
            = newUpdater(AbstractCompletableFuture.class, Object.class, "state");

    // This field is only assigned by the STATE AtomicReferenceFieldUpdater.
    // This field encodes an option type of 2 types, an ExecutionCallbackNode or a result.
    // if the state is an instance of the former, then the future is not done. Otherwise it is.
    // The reason for this abuse of the type system is to deal with the head node and the result
    // in a single cas. Using a single cas prevent a thread that calls and then do this concurrently
    // with a thread that calls setResult.
    private volatile Object state = INITIAL_STATE;

    private final ILogger logger;
    private final Executor defaultExecutor;

    protected AbstractCompletableFuture(NodeEngine nodeEngine, ILogger logger) {
        this(nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR), logger);
    }

    protected AbstractCompletableFuture(Executor defaultExecutor, ILogger logger) {
        this.defaultExecutor = defaultExecutor;
        this.logger = logger;
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, defaultExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        for (; ; ) {
            Object currentState = this.state;

            if (isCancelledState(currentState)) {
                return;
            }

            if (isDoneState(currentState)) {
                runAsynchronous(callback, executor, currentState);
                return;
            }

            ExecutionCallbackNode newState
                    = new ExecutionCallbackNode<V>(callback, executor, (ExecutionCallbackNode) currentState);

            if (STATE.compareAndSet(this, currentState, newState)) {
                // we have successfully scheduled the callback.
                return;
            }

            // we failed to update the state. This can mean 2 things:
            // either a result was set, which we'll see when retrying this loop
            // or a different thread also called andThen, which we'll deal with when we retry the loop.
        }
    }

    @Override
    public boolean isDone() {
        return isDoneState(state);
    }

    /**
     * Returns {@code true} if the task with the given state completed - analogously to the Future's contract.
     * Completion may be due to normal termination, an exception, or
     * cancellation -- in all of these cases, this method will return
     * {@code true}.
     *
     * @return {@code true} if this task completed
     */
    private static boolean isDoneState(Object state) {
        return !(state instanceof ExecutionCallbackNode);
    }

    @Override
    public final boolean cancel(boolean mayInterruptIfRunning) {
        Boolean shouldCancel = null;

        for (; ; ) {
            Object currentState = this.state;

            if (isDoneState(currentState)) {
                return false;
            }

            if (shouldCancel == null) {
                shouldCancel = shouldCancel(mayInterruptIfRunning);
            }
            if (!shouldCancel) {
                return false;
            }

            if (STATE.compareAndSet(this, currentState, CANCELLED_STATE)) {
                cancelled(mayInterruptIfRunning);
                notifyThreadsWaitingOnGet();
                return true;
            }
        }
    }

    /**
     * Protected method invoked on cancel(). Enables aborting the cancellation.
     * Useful for futures' encompassing logic that can forbid the cancellation.
     * By default always returns true.
     *
     * @param mayInterruptIfRunning passed through from cancel call
     * @return true should the cancellation proceed; false otherwise
     */
    protected boolean shouldCancel(boolean mayInterruptIfRunning) {
        return true;
    }

    /**
     * Protected method invoked when this task transitions to state
     * {@code isCancelled}. The default implementation does nothing.
     * Subclasses may override this method to invoke callbacks or perform
     * bookkeeping. Implementation has to handle exceptions itself.
     *
     * @param mayInterruptIfRunning {@code true} if the thread executing this
     *                              task was supposed to be interrupted; otherwise, in-progress tasks are allowed
     *                              to complete
     */
    protected void cancelled(boolean mayInterruptIfRunning) {
    }

    private static boolean isCancelledState(Object state) {
        return state == CANCELLED_STATE;
    }

    @Override
    public boolean isCancelled() {
        return isCancelledState(state);
    }

    @Override
    public final V get() throws InterruptedException, ExecutionException {
        for (; ; ) {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException ignored) {
                // A timeout here can only be a spurious artifact.
                // It should never happen and even if it does, we must retry.
                EmptyStatement.ignore(ignored);
            }
        }
    }


    /**
     * PLEASE NOTE: It's legal to override this method, but please bear in mind that you should call super.get() or
     * implement the done() and cancelled() callbacks to be notified if this future gets done or cancelled.
     * Otherwise the overridden implementation of get() may get stuck on waiting forever.
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
        final long deadlineTimeMillis = System.currentTimeMillis() + unit.toMillis(timeout);
        long millisToWait;
        for (; ; ) {
            Object currentState = this.state;

            if (isCancelledState(currentState)) {
                throw new CancellationException();
            }
            if (isDoneState(currentState)) {
                return getResult(currentState);
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }

            millisToWait = deadlineTimeMillis - System.currentTimeMillis();
            if (millisToWait <= 0) {
                throw new TimeoutException();
            }

            synchronized (this) {
                if (!isDoneState(this.state)) {
                    this.wait(millisToWait);
                }
            }
        }
    }

    protected void setResult(Object result) {
        for (; ; ) {
            Object currentState = this.state;

            if (isDoneState(currentState)) {
                return;
            }

            if (STATE.compareAndSet(this, currentState, result)) {
                done();
                notifyThreadsWaitingOnGet();
                runAsynchronous((ExecutionCallbackNode) currentState, result);
                break;
            }
        }
    }

    /**
     * Protected method invoked when this task transitions to state
     * {@code isDone} (only normally - in case of cancellation cancelled() is invoked)
     * The default implementation does nothing.
     * Subclasses may override this method to invoke callbacks or perform
     * bookkeeping. Implementation has to handle exceptions itself.
     */
    protected void done() {
    }

    /**
     * Returns:
     * <ul>
     * <li>null - if cancelled or not done</li>
     * <li>result - if done and result is NOT Throwable</li>
     * <li>sneaky throws an exception - if done and result is Throwable</li>
     * </ul>
     */
    protected V getResult() {
        return getResult(this.state);
    }

    private static <V> V getResult(Object state) {
        if (isCancelledState(state)) {
            return null;
        }

        if (!isDoneState(state)) {
            return null;
        }

        if (state instanceof Throwable) {
            sneakyThrow((Throwable) state);
        }

        return (V) state;
    }

    private void notifyThreadsWaitingOnGet() {
        synchronized (this) {
            this.notifyAll();
        }
    }

    private void runAsynchronous(ExecutionCallbackNode head, Object result) {
        while (head != INITIAL_STATE) {
            runAsynchronous(head.callback, head.executor, result);
            head = head.next;
        }
    }

    private void runAsynchronous(ExecutionCallback<V> callback, Executor executor, Object result) {
        executor.execute(new ExecutionCallbackRunnable<V>(getClass(), result, callback, logger));
    }

    private static final class ExecutionCallbackNode<E> {
        final ExecutionCallback<E> callback;
        final Executor executor;
        final ExecutionCallbackNode<E> next;

        private ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

    private static final class ExecutionCallbackRunnable<V> implements Runnable {
        private final Class<?> caller;
        private final Object result;
        private final ExecutionCallback<V> callback;
        private final ILogger logger;

        public ExecutionCallbackRunnable(Class<?> caller, Object result, ExecutionCallback<V> callback, ILogger logger) {
            this.caller = caller;
            this.result = result;
            this.callback = callback;
            this.logger = logger;
        }

        @Override
        public void run() {
            try {
                if (result instanceof Throwable) {
                    callback.onFailure((Throwable) result);
                } else {
                    callback.onResponse((V) result);
                }
            } catch (Throwable cause) {
                logger.severe("Failed asynchronous execution of execution callback: " + callback
                        + "for call " + caller, cause);
            }
        }
    }
}
