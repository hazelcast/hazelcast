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

package com.hazelcast.core;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * A Future where one can asynchronously listen on completion. This functionality is needed for the
 * reactive programming model.
 * <p>
 * For more information see:
 * <a href='https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html'>
 * https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/CompletableFuture.html</a>
 * <p>
 * This class can be dropped once Hazelcast relies on Java8+. It is added to make Hazelcast compatible
 * with Java6/7.
 *
 * @param <V>
 * @since 3.2
 */
public interface ICompletableFuture<V> extends Future<V> {

    /**
     * Registers a callback that will run after this future is completed. If
     * this future is already completed, it runs immediately.
     *
     * <p>Please note that there is no ordering guarantee for running multiple
     * callbacks. It is also not guaranteed that the callback will run within
     * the same thread that completes the future.
     *
     * @param callback the callback to execute
     */
    void andThen(ExecutionCallback<V> callback);

    /**
     * Registers a callback that will run with the provided executor after this
     * future is completed. If this future is already completed, it runs
     * immediately. The callback is called using the given {@code executor}.
     *
     * Please note that there is no ordering guarantee for executing multiple
     * callbacks.
     *
     * @param callback the callback to execute
     * @param executor the executor in which the callback will be run
     */
    void andThen(ExecutionCallback<V> callback, Executor executor);
}
