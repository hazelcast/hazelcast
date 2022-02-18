/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.executor;

import com.hazelcast.spi.impl.DeserializingCompletableFuture;

import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static java.util.concurrent.atomic.AtomicReferenceFieldUpdater.newUpdater;

public class CompletableFutureTask<V> extends DeserializingCompletableFuture<V>
        implements RunnableFuture<V> {

    private static final AtomicReferenceFieldUpdater<CompletableFutureTask, Thread> RUNNER
            = newUpdater(CompletableFutureTask.class, Thread.class, "runner");

    private final Callable<V> callable;

    private volatile Thread runner;

    CompletableFutureTask(Callable<V> callable) {
        super();
        this.callable = callable;
    }

    CompletableFutureTask(Runnable runnable, V result) {
        super();
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
                    complete((V) result);
                } catch (Throwable ex) {
                    completeExceptionally(ex);
                }
            }
        } finally {
            // runner must be non-null until state is settled in setResult() to
            // prevent concurrent calls to run()
            runner = null;
        }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        boolean result = super.cancel(mayInterruptIfRunning);
        // additionally handle the interruption of the executing thread
        if (result && mayInterruptIfRunning) {
            Thread executingThread = runner;
            if (executingThread != null) {
                executingThread.interrupt();
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return "CompletableFutureTask{"
                + "callable=" + callable
                + ", runner=" + runner
                + '}';
    }
}
