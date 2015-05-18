/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public abstract class AbstractCompletableFuture<V> implements ICompletableFuture<V> {

    protected static final Object NOT_AVAILABLE_VALUE = new Object();
    protected final NodeEngine nodeEngine;

    protected volatile Object result = NOT_AVAILABLE_VALUE;

    private final Object completionLock = new Object();

    private final ILogger logger;
    private volatile ExecutionCallbackNode<V> callbackHead;

    protected AbstractCompletableFuture(NodeEngine nodeEngine, ILogger logger) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        // no need to lock if the future is already completed
        // isDone() is not called because it can be overridden by subclasses
        if (isDoneInternal()) {
            runAsynchronous(callback, executor);
            return;
        }

        synchronized (completionLock) {
            // isDone() is not called because it can be overridden by subclasses
            if (isDoneInternal()) {
                runAsynchronous(callback, executor);
                return;
            }

            this.callbackHead = new ExecutionCallbackNode<V>(callback, executor, callbackHead);
        }
    }

    @Override
    public boolean isDone() {
        return isDoneInternal();
    }

    private boolean isDoneInternal() {
        return result != NOT_AVAILABLE_VALUE;
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
        ExecutionCallbackNode<V> callbackChain;
        synchronized (completionLock) {
            // isDone() is not called because it can be overridden by subclasses
            if (isDoneInternal()) {
                return;
            }

            this.result = result;
            callbackChain = callbackHead;
            callbackHead = null;
        }

        fireCallbacks(callbackChain);
    }

    protected V getResult() {
        Object result = this.result;
        if (result instanceof Throwable) {
            ExceptionUtil.sneakyThrow((Throwable) result);
        }
        return (V) result;
    }

    protected void fireCallbacks(ExecutionCallbackNode<V> callbackChain) {
        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    private void runAsynchronous(final ExecutionCallback<V> callback, final Executor executor) {
        try {
            final Object result = this.result;
            executor.execute(new Runnable() {
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
            });
        } catch (RejectedExecutionException e) {
            logger.warning("Execution of callback: " + callback + " is rejected!", e);
        }
    }

    protected ExecutorService getAsyncExecutor() {
        return nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
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
