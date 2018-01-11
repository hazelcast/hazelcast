/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.scheduledexecutor.impl.NamedTaskDecorator;
import com.hazelcast.spi.annotation.Beta;

import java.util.concurrent.Callable;

/**
 * A helper class with utilities to act upon {@link Runnable} and/or {@link Callable} tasks.
 */
@Beta
public final class TaskUtils {

    private TaskUtils() {
    }

    /**
     * Decorate any {@link Runnable} with a {@link NamedTask} to provide naming information to scheduler.
     *
     * @param name     The name that the task will have
     * @param runnable The runnable task to be named
     * @return A new Runnable implementing the {@link NamedTask} interface
     */
    public static Runnable named(final String name, final Runnable runnable) {
        return NamedTaskDecorator.named(name, runnable);
    }

    /**
     * Decorate any {@link Callable} with a {@link NamedTask} to provide naming information to scheduler.
     *
     * @param name     The name that the task will have
     * @param callable The callable task to be named
     * @return A new Callable implementing the {@link NamedTask} interface
     */
    public static <V> Callable<V> named(final String name, final Callable<V> callable) {
        return NamedTaskDecorator.named(name, callable);
    }
}
