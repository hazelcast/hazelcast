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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public abstract class AbstractCompletableFuture<V> implements ICompletableFuture<V> {

    protected static final Object NULL_VALUE = new Object();
    protected final AtomicReferenceFieldUpdater<AbstractCompletableFuture, Object> resultUpdater;
    protected final NodeEngine nodeEngine;
    // This field is only assigned by the atomic updater
    protected volatile Object result = NULL_VALUE;

    private final AtomicReferenceFieldUpdater<AbstractCompletableFuture, ExecutionCallbackNode> callbackUpdater;
    private final ILogger logger;
    private volatile ExecutionCallbackNode<V> callbackHead;

    protected AbstractCompletableFuture(NodeEngine nodeEngine, ILogger logger) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
        this.callbackUpdater = AtomicReferenceFieldUpdater.newUpdater(
                AbstractCompletableFuture.class, ExecutionCallbackNode.class, "callbackHead");
        this.resultUpdater = AtomicReferenceFieldUpdater.newUpdater(
                AbstractCompletableFuture.class, Object.class, "result");
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        if (isDone()) {
            runAsynchronous(callback, executor);
            return;
        }
        for (;;) {
            ExecutionCallbackNode oldCallbackHead = callbackHead;
            ExecutionCallbackNode newCallbackHead = new ExecutionCallbackNode<V>(callback, executor, oldCallbackHead);
            if (callbackUpdater.compareAndSet(this, oldCallbackHead, newCallbackHead)) {
                break;
            }
        }
    }

    @Override
    public boolean isDone() {
        return result != NULL_VALUE;
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
        if (resultUpdater.compareAndSet(this, NULL_VALUE, result)) {
            fireCallbacks();
        }
    }

    protected V getResult() {
        Object result = this.result;
        if (result instanceof Throwable) {
            ExceptionUtil.sneakyThrow((Throwable) result);
        }
        return (V) result;
    }

    protected void fireCallbacks() {
        ExecutionCallbackNode<V> callbackChain;
        for (;;) {
            callbackChain = callbackHead;
            if (callbackUpdater.compareAndSet(this, callbackChain, null)) {
                break;
            }
        }
        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    private void runAsynchronous(final ExecutionCallback<V> callback, final Executor executor) {
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
                } catch (Throwable t) {
                    //todo: improved error message
                    logger.severe("Failed to async for " + AbstractCompletableFuture.this, t);
                }
            }
        });
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
