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

package com.hazelcast.spi.impl.executionservice.impl;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Decorator to prevent concurrent task execution.
 *
 * If this task is running on a thread and another thread calls attempt the execute it concurrently
 * then the 2nd execution will be skipped.
 *
 */
public class SkipOnConcurrentExecutionDecorator implements Runnable {
    private final AtomicBoolean isAlreadyRunning = new AtomicBoolean();
    private final Runnable runnable;

    public SkipOnConcurrentExecutionDecorator(Runnable runnable) {
        this.runnable = runnable;
    }

    @Override
    public void run() {
        if (isAlreadyRunning.compareAndSet(false, true)) {
            try {
                runnable.run();
            } finally {
                isAlreadyRunning.set(false);
            }
        }
    }
}
