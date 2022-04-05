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

package com.hazelcast.durableexecutor;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;

/**
 * A Future where one can obtain the task ID for tracking the response.
 *
 * @param <V> The result type returned by this Future's {@code get} method
 */
public interface DurableExecutorServiceFuture<V> extends CompletionStage<V>, Future<V> {

    /**
     * A unique ID for the executing task
     *
     * @return the task ID
     */
    long getTaskId();
}
