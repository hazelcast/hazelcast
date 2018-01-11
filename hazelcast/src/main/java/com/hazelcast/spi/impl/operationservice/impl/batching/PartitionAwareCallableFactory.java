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

package com.hazelcast.spi.impl.operationservice.impl.batching;

/**
 * Factory used to instantiate {@link PartitionAwareCallable} instances
 *
 * @see PartitionAwareCallable
 * @see PartitionAwareCallableBatchingRunnable
 * @since 3.9
 */
public interface PartitionAwareCallableFactory<V> {

    /**
     * Factory method that instantiates {@link PartitionAwareCallable} instances
     *
     * @return a new instance of a concrete implmentation of the PartitionAwareCallable
     */
    PartitionAwareCallable<V> create();

}
