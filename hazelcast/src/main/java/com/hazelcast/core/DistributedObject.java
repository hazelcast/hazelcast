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

/**
 * Base interface for all distributed objects.
 *
 * @see IMap
 * @see IQueue
 * @see MultiMap
 * @see ITopic
 * @see ILock
 * @see IExecutorService
 * @see TransactionalMap
 * @see TransactionalQueue
 * @see TransactionalMultiMap
 */
public interface DistributedObject {

    /**
     * Returns the unique id for this object.
     *
     * @return id the of this instance
     * @deprecated  will be removed in Hazelcast 3.1. Use {@link #getName} instead.
     */
    Object getId();

    /**
     * Returns the key of partition this DistributedObject is assigned to. The returned value only has meaning
     * for a non partitioned data-structure like an IAtomicLong. For a partitioned data-structure like an IMap
     * the returned value will not be null, but otherwise undefined.
     *
     * @return the partition key.
     */
    String getPartitionKey();

    /**
     * Returns the name that uniquely identifies this DistributedObject. Returned value will never be null.
     *
     * @return the name for this object.
     */
    String getName();

    /**
     * Returns the service name for this object.
     */
    String getServiceName();

    /**
     * Destroys this object cluster-wide.
     * Clears and releases all resources for this object.
     */
    void destroy();
}
