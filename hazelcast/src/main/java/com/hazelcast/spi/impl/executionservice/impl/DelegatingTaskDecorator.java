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

import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Executor;

/**
 * Delegates task execution to a given Executor.
 *
 * This is convenient when you want to start a task, do not want the task to block a caller thread.
 *
 */
class DelegatingTaskDecorator implements Runnable {

    private final Executor executor;
    private final Runnable runnable;

    /**
     * @param runnable Task to be executed
     * @param executor Executor the task to be delegated to
     */
    public DelegatingTaskDecorator(Runnable runnable, Executor executor) {
        this.executor = executor;
        this.runnable = runnable;
    }

    @Override
    public void run() {
        try {
            executor.execute(runnable);
        } catch (Throwable t) {
            ExceptionUtil.sneakyThrow(t);
        }
    }

}
