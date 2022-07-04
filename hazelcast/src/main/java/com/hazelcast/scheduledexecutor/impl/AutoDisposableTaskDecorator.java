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

package com.hazelcast.scheduledexecutor.impl;

import com.hazelcast.scheduledexecutor.AutoDisposableTask;

import java.util.concurrent.Callable;

public class AutoDisposableTaskDecorator<V>
        extends AbstractTaskDecorator<V> implements AutoDisposableTask {


    AutoDisposableTaskDecorator() {
    }

    private AutoDisposableTaskDecorator(Runnable runnable) {
        super(runnable);
    }

    private AutoDisposableTaskDecorator(Callable<V> callable) {
        super(callable);
    }


    @Override
    public int getClassId() {
        return ScheduledExecutorDataSerializerHook.AUTO_DISPOSABLE_TASK_DECORATOR;
    }


    public static Runnable autoDisposable(Runnable runnable) {
        return new AutoDisposableTaskDecorator(runnable);
    }

    public static <V> Callable<V> autoDisposable(Callable<V> callable) {
        return new AutoDisposableTaskDecorator<V>(callable);
    }
}
