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

import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.ExecutionServiceImpl.CompletableFutureEntry;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.*;

import static com.hazelcast.util.ValidationUtil.isNotNull;

class BasicCompletableFuture<V> implements CompletableFuture<V> {

    private static final Object NULL_VALUE = new Object();

    private final ILogger logger;
    private final Future<V> future;
    private final NodeEngine nodeEngine;
    private final CompletableFutureEntry<V> completableFutureEntry;
    private volatile ExecutionCallbackNode<V> callbackHead;
    private volatile Object result = NULL_VALUE;

    BasicCompletableFuture(Future<V> future, NodeEngine nodeEngine,
                                  CompletableFutureEntry<V> completableFutureEntry) {
        this.future = future;
        this.nodeEngine = nodeEngine;
        this.completableFutureEntry = completableFutureEntry;
        this.logger = nodeEngine.getLogger(BasicCompletableFuture.class);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, getAsyncExecutor());
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        synchronized (this) {
            if (isDone()) {
                runAsynchronous(callback, executor);
                return;
            }

            this.callbackHead = new ExecutionCallbackNode<V>(callback, executor, callbackHead);
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
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

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        // Cancel the async waiting since we now have a waiting thread to use
        completableFutureEntry.cancel();

        // Waiting for the result and fire callbacks afterwards
        future.get(timeout, unit);
        fireCallbacks();
        return (V) result;
    }

    public void fireCallbacks() {
        ExecutionCallbackNode<V> callbackChain;
        synchronized (this) {
            callbackChain = callbackHead;
            callbackHead = null;
        }

        while (callbackChain != null) {
            runAsynchronous(callbackChain.callback, callbackChain.executor);
            callbackChain = callbackChain.next;
        }
    }

    private Object readResult() {
        if (result == NULL_VALUE) {
            try {
                result = future.get();
            } catch (Throwable t) {
                result = t;
            }
        }
        return result;
    }

    private void runAsynchronous(final ExecutionCallback<V> callback, final Executor executor) {
        final Object result = readResult();
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
                    logger.severe("Failed to async for " + BasicCompletableFuture.this, t);
                }
            }
        });
    }

    private ExecutorService getAsyncExecutor() {
        return nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private static class ExecutionCallbackNode<E> {
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
