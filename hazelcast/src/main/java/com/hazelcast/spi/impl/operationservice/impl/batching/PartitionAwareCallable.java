/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl.batching;

/**
 * Partition callable that is guaranteed to have an exclusive Read-Write access to partition data.
 * Typically created by the {@link PartitionAwareCallableFactory}
 *
 * @param <V> type of result
 * @see PartitionAwareCallableFactory
 * @see PartitionAwareCallableBatchingRunnable
 * @since 3.9
 */
public interface PartitionAwareCallable<V> {

    /**
     * @param partitionId partitionId of the partition the callable is executed for
     * @return call result
     */
    V call(int partitionId);

}
