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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.util.ValidationUtil.isNotNull;
import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public abstract class AbstractCompletableFuture<V> implements ICompletableFuture<V> {

    static final Object INITIAL_STATE = new ExecutionCallbackNode(null, null, null);

    private static final AtomicReferenceFieldUpdater<AbstractCompletableFuture, Object> STATE
            = newUpdater(AbstractCompletableFuture.class, Object.class, "state");

    // This field is only assigned by the STATE AtomicReferenceFieldUpdater.
    // This field encodes an option type of 2 types, an ExecutionCallbackNode or a result.
    // if the state is an instance of the former, then the future is not done. Otherwise it is.
    // The reason for this abuse of the type system is to deal with the head node and the result
    // in a single cas. Using a single cas prevent a thread that calls and then do this concurrently
    // with a thread that calls setResult.
    volatile Object state = INITIAL_STATE;

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

            if (isDone(currentState)) {
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
        return isDone(state);
    }

    private boolean isDone(Object state) {
        return !(state instanceof ExecutionCallbackNode);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        try {
            return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            logger.severe("Unexpected timeout while processing " + this, e);
            return null;
        }
    }

    public void setResult(Object result) {
        for (; ; ) {
            Object currentState = this.state;

            if (isDone(currentState)) {
                return;
            }

            if (STATE.compareAndSet(this, currentState, result)) {
                runAsynchronous((ExecutionCallbackNode) currentState, result);
                break;
            }
        }
    }

    protected V getResult() {
        Object state = this.state;

        if (!isDone(state)) {
            return null;
        }

        if (state instanceof Throwable) {
            sneakyThrow((Throwable) state);
        }

        return (V) state;
    }

    protected void runAsynchronous(ExecutionCallbackNode head, Object result) {
        while (head != INITIAL_STATE) {
            runAsynchronous(head.callback, head.executor, result);
            head = head.next;
        }
    }

    private void runAsynchronous(ExecutionCallback<V> callback, Executor executor, Object result) {
        executor.execute(new ExecutionCallbackRunnable<V>(result, callback));
    }

    static final class ExecutionCallbackNode<E> {
        final ExecutionCallback<E> callback;
        final Executor executor;
        final ExecutionCallbackNode<E> next;

        private ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

    private class ExecutionCallbackRunnable<V> implements Runnable {
        private final Object result;
        private final ExecutionCallback<V> callback;

        public ExecutionCallbackRunnable(Object result, ExecutionCallback<V> callback) {
            this.result = result;
            this.callback = callback;
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
                        + "for call " + AbstractCompletableFuture.this, cause);
            }
        }
    }
}
