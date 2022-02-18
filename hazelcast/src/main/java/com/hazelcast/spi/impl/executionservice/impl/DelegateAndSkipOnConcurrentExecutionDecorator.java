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

package com.hazelcast.spi.impl.executionservice.impl;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Decorator to delegate task to an executor and prevent concurrent task execution.
 *
 * If this task is running on a thread and another thread calls attempt the execute it concurrently
 * then the 2nd execution will be skipped.
 */
public class DelegateAndSkipOnConcurrentExecutionDecorator implements Runnable {

    private final AtomicBoolean isAlreadyRunning = new AtomicBoolean();
    private final Runnable runnable;
    private final Executor executor;
    private volatile Throwable throwable;

    public DelegateAndSkipOnConcurrentExecutionDecorator(Runnable runnable, Executor executor) {
        this.runnable = new DelegateDecorator(runnable);
        this.executor = executor;
    }

    @Override
    public void run() {
        if (isAlreadyRunning.compareAndSet(false, true)) {
            if (throwable != null) {
                // Capturing & throwing the exception to propagate the failure to the scheduler (suppress future executions)
                // instead of hiding in the delegated executor.
                rethrow(throwable);
                return;
            }

            executor.execute(runnable);
        }
    }

    @Override
    public String toString() {
        return "DelegateAndSkipOnConcurrentExecutionDecorator{"
                + "isAlreadyRunning=" + isAlreadyRunning
                + ", runnable=" + runnable
                + ", executor=" + executor
                + ", throwable=" + throwable
                + '}';
    }

    private class DelegateDecorator implements Runnable {

        private final Runnable runnable;

        DelegateDecorator(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Throwable t) {
                throwable = t;
            } finally {
                isAlreadyRunning.set(false);
            }
        }

        @Override
        public String toString() {
            return "DelegateDecorator{runnable=" + runnable + '}';
        }
    }
}
