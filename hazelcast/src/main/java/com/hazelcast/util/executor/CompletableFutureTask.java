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

package com.hazelcast.util.executor;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.Logger;
import com.hazelcast.spi.impl.AbstractCompletableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class CompletableFutureTask<V> extends AbstractCompletableFuture<V> implements ICompletableFuture<V>, RunnableFuture<V> {

    private static final AtomicReferenceFieldUpdater<CompletableFutureTask, Thread> RUNNER
            = newUpdater(CompletableFutureTask.class, Thread.class, "runner");

    private final Callable<V> callable;

    private volatile Thread runner;

    public CompletableFutureTask(Callable<V> callable, ExecutorService asyncExecutor) {
        super(asyncExecutor, Logger.getLogger(CompletableFutureTask.class));
        this.callable = callable;
    }

    public CompletableFutureTask(Runnable runnable, V result, ExecutorService asyncExecutor) {
        super(asyncExecutor, Logger.getLogger(CompletableFutureTask.class));
        this.callable = Executors.callable(runnable, result);
    }

    @Override
    public void run() {
        if (isDone()) {
            return;
        }

        if (runner != null || !RUNNER.compareAndSet(this, null, Thread.currentThread())) {
            // prevents concurrent calls to run
            return;
        }

        try {
            Callable c = callable;
            if (c != null) {
                Object result = null;
                try {
                    result = c.call();
                } catch (Throwable ex) {
                    result = new ExecutionException(ex);
                } finally {
                    setResult(result);
                }
            }
        } finally {
            // runner must be non-null until state is settled in setResult() to
            // prevent concurrent calls to run()
            runner = null;
        }
    }

    @Override
    protected void cancelled(boolean mayInterruptIfRunning) {
        // additionally handle the interruption of the executing thread
        if (mayInterruptIfRunning) {
            Thread executingThread = runner;
            if (executingThread != null) {
                executingThread.interrupt();
            }
        }
    }

}
