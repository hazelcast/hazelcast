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

import com.hazelcast.internal.util.ExceptionUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Delegates task execution to a given Executor.
 *
 * This is convenient when you want to start a task, do not want the task to block a caller thread.
 *
 */
class DelegatingCallableTaskDecorator<V>
        implements Callable<Future<V>> {

    private final ExecutorService executor;
    private final Callable<V> callable;

    /**
     * @param callable Task to be executed
     * @param executor ExecutorService the task to be delegated to
     */
    DelegatingCallableTaskDecorator(Callable<V> callable, ExecutorService executor) {
        this.executor = executor;
        this.callable = callable;
    }

    @Override
    public Future<V> call() {
        try {
            return executor.submit(callable);
        } catch (Throwable t) {
            ExceptionUtil.sneakyThrow(t);
        }

        return null;
    }

}
