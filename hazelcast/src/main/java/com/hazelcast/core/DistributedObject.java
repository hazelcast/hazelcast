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

package com.hazelcast.core;

import com.hazelcast.collection.IQueue;
import com.hazelcast.cp.IAtomicLong;
import com.hazelcast.map.IMap;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.spi.tenantcontrol.DestroyEventContext;
import com.hazelcast.topic.ITopic;
import com.hazelcast.transaction.TransactionalMap;
import com.hazelcast.transaction.TransactionalMultiMap;
import com.hazelcast.transaction.TransactionalQueue;

import javax.annotation.Nonnull;

/**
 * Base interface for all distributed objects.
 *
 * All distributed objects are not garbage collectable unless {@link #destroy()} is called first.
 * Note: Failure to destroy after you are done using a distributed object will lead to memory leaks.
 *
 * @see IMap
 * @see IQueue
 * @see MultiMap
 * @see ITopic
 * @see IExecutorService
 * @see TransactionalMap
 * @see TransactionalQueue
 * @see TransactionalMultiMap
 */
public interface DistributedObject {

    /**
     * Returns the key of the partition that this DistributedObject is assigned to. The returned value only has meaning
     * for a non-partitioned data structure like an {@link IAtomicLong}. For a partitioned data structure like an {@link IMap},
     * the returned value will not be null, but otherwise undefined.
     *
     * @return the partition key.
     */
    String getPartitionKey();

    /**
     * Returns the unique name for this DistributedObject. The returned value will never be null.
     *
     * The suggested way for getting name is retrieving it through
     * {@link com.hazelcast.core.DistributedObjectUtil#getName(DistributedObject)}
     * because this might be also a {@link com.hazelcast.core.PrefixedDistributedObject}.
     *
     * @return the unique name for this object.
     */
    String getName();

    /**
     * Returns the service name for this object.
     *
     * @return the service name for this object.
     */
    String getServiceName();

    /**
     * Destroys this object cluster-wide.
     * Clears and releases all resources for this object.
     */
    void destroy();

    /**
     * Returns a hook which can be used by tenant control implementation to clean
     * up resources once a tenant is destroyed.
     * <p>
     * This hook is used, for example, when a distributed object needs to clear any
     * cached classes related to the destroyed tenant and to avoid class loader
     * leaks and {@link ClassNotFoundException}s when the tenant is destroyed.
     *
     * @return destroy context, cannot be null
     */
    default @Nonnull DestroyEventContext getDestroyContextForTenant() {
        return () -> { };
    }
}
