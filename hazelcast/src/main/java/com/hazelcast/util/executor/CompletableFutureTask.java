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

package com.hazelcast.util.executor;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ValidationUtil.isNotNull;

public class CompletableFutureTask<V> extends FutureTask<V> implements ICompletableFuture<V> {

    private final AtomicReferenceFieldUpdater<CompletableFutureTask, ExecutionCallbackNode> callbackUpdater;
    private final ILogger logger = Logger.getLogger(CompletableFutureTask.class);
    private final ExecutorService asyncExecutor;

    private volatile ExecutionCallbackNode<V> callbackHead;

    public CompletableFutureTask(Callable<V> callable, ExecutorService asyncExecutor) {
        super(callable);
        this.asyncExecutor = asyncExecutor;
        this.callbackUpdater = AtomicReferenceFieldUpdater.newUpdater(
                CompletableFutureTask.class, ExecutionCallbackNode.class, "callbackHead");
    }

    public CompletableFutureTask(Runnable runnable, V result, ExecutorService asyncExecutor) {
        super(runnable, result);
        this.asyncExecutor = asyncExecutor;
        this.callbackUpdater = AtomicReferenceFieldUpdater.newUpdater(
                CompletableFutureTask.class, ExecutionCallbackNode.class, "callbackHead");
    }

    @Override
    public void run() {
        try {
            super.run();
        } finally {
            fireCallbacks();
        }
    }

    @Override
    public void andThen(ExecutionCallback<V> callback) {
        andThen(callback, asyncExecutor);
    }

    @Override
    public void andThen(ExecutionCallback<V> callback, Executor executor) {
        isNotNull(callback, "callback");
        isNotNull(executor, "executor");

        if (isDone()) {
            runAsynchronous(callback, executor);
            return;
        }
        for ( ;;) {
            ExecutionCallbackNode<V> oldCallbackHead = callbackHead;
            ExecutionCallbackNode<V> newCallbackHead = new ExecutionCallbackNode<V>(callback, executor, oldCallbackHead);
            if (callbackUpdater.compareAndSet(this, oldCallbackHead, newCallbackHead)) {
                break;
            }
        }
    }

    private Object readResult() {
        try {
            return get();
        } catch (Throwable t) {
            return t;
        }
    }

    private void fireCallbacks() {
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
                    logger.severe("Failed to async for " + CompletableFutureTask.this, t);
                }
            }
        });
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
