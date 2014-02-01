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

package com.hazelcast.core;

import com.hazelcast.spi.annotation.Beta;

import java.util.concurrent.Executor;
import java.util.concurrent.Future;

/**
 * A Future where one can asynchronously listen on completion. This functionality is needed for the
 * reactive programming model.
 *
 * For more information see:
 * http://download.java.net/jdk8/docs/api/java/util/concurrent/CompletableFuture.html
 *
 * This class can be dropped once Hazelcast relies on Java8+. It is added to make Hazelcast compatible
 * with Java6/7.
 *
 * @param <V>
 * @since 3.2
 */
@Beta
public interface ICompletableFuture<V> extends Future<V> {

    void andThen(ExecutionCallback<V> callback);

    void andThen(ExecutionCallback<V> callback, Executor executor);
}
