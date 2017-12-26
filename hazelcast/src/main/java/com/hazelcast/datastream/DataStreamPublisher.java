/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.core.ICompletableFuture;

/**
 * A publisher for the {@link DataStream}.
 *
 * @param <R>
 */
public interface DataStreamPublisher<R> {

    /**
     * Publishes on a random partition.
     *
     * @param record the record to publish.
     */
    void publish(R record);

    /**
     * Publishes the record on the specified partition.
     *
     * @param partitionKey
     * @param record
     * @param <P>
     */
    <P> void publish(P partitionKey, R record);

    <P> ICompletableFuture publishAsync(P partitionKey, R record);
}
