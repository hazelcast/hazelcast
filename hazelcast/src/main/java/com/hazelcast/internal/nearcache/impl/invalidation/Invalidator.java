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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.partition.IPartitionService;

import java.util.Collection;
import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.internal.util.ToHeapDataConverter.toHeapData;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Contains shared functionality for Near Cache invalidation.
 */
public abstract class Invalidator {

    protected final int partitionCount;
    protected final String serviceName;
    protected final ILogger logger;
    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final MetaDataGenerator metaDataGenerator;
    protected final IPartitionService partitionService;
    protected final Function<EventRegistration, Boolean> eventFilter;

    public Invalidator(String serviceName, Function<EventRegistration, Boolean> eventFilter, NodeEngine nodeEngine) {
        this.serviceName = serviceName;
        this.eventFilter = eventFilter;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.eventService = nodeEngine.getEventService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.metaDataGenerator = new MetaDataGenerator(partitionCount);
    }

    protected abstract void invalidateInternal(Invalidation invalidation, int orderKey);

    /**
     * Invalidates supplied key from Near Caches of supplied data structure name.
     *
     * @param key               key of the entry to be removed from Near Cache
     * @param dataStructureName name of the data structure to be invalidated
     */
    public final void invalidateKey(Data key, String dataStructureName, UUID sourceUuid) {
        checkNotNull(key, "key cannot be null");
        checkNotNull(sourceUuid, "sourceUuid cannot be null");

        Invalidation invalidation = newKeyInvalidation(key, dataStructureName, sourceUuid);
        invalidateInternal(invalidation, getPartitionId(key));
    }

    /**
     * Invalidates all keys from Near Caches of supplied data structure name.
     *
     * @param dataStructureName name of the data structure to be cleared
     */
    public final void invalidateAllKeys(String dataStructureName, UUID sourceUuid) {
        checkNotNull(sourceUuid, "sourceUuid cannot be null");

        int orderKey = getPartitionId(dataStructureName);
        Invalidation invalidation = newClearInvalidation(dataStructureName, sourceUuid);
        sendImmediately(invalidation, orderKey);
    }

    public final MetaDataGenerator getMetaDataGenerator() {
        return metaDataGenerator;
    }

    public final void forceIncrementSequence(String dataStructureName, int partitionId) {
        MetaDataGenerator metaDataGenerator = getMetaDataGenerator();
        metaDataGenerator.nextSequence(dataStructureName, partitionId);
    }

    private Invalidation newKeyInvalidation(Data key, String dataStructureName, UUID sourceUuid) {
        int partitionId = getPartitionId(key);
        return newInvalidation(key, dataStructureName, sourceUuid, partitionId);
    }

    private Invalidation newClearInvalidation(String dataStructureName, UUID sourceUuid) {
        int partitionId = getPartitionId(dataStructureName);
        return newInvalidation(null, dataStructureName, sourceUuid, partitionId);
    }

    protected Invalidation newInvalidation(Data key, String dataStructureName, UUID sourceUuid, int partitionId) {
        long sequence = metaDataGenerator.nextSequence(dataStructureName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        return new SingleNearCacheInvalidation(toHeapData(key), dataStructureName, sourceUuid, partitionUuid, sequence);
    }

    private int getPartitionId(Data o) {
        return partitionService.getPartitionId(o);
    }

    private int getPartitionId(Object o) {
        return partitionService.getPartitionId(o);
    }

    protected final void sendImmediately(Invalidation invalidation, int orderKey) {
        String dataStructureName = invalidation.getName();

        Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, dataStructureName);
        for (EventRegistration registration : registrations) {
            if (eventFilter.apply(registration)) {
                eventService.publishEvent(serviceName, registration, invalidation, orderKey);
            }
        }
    }

    /**
     * Removes supplied data structures invalidation queues and flushes their content.
     * This method is called when removing a Near Cache for example with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param dataStructureName name of the data structure.
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    public void destroy(String dataStructureName, UUID sourceUuid) {
        invalidateAllKeys(dataStructureName, sourceUuid);
        metaDataGenerator.destroyMetaDataFor(dataStructureName);
    }

    /**
     * Resets this invalidator back to its initial state.
     * Aimed to call with {@link ManagedService#reset()}
     *
     * @see ManagedService#reset()
     */
    public void reset() {
        // nop
    }

    /**
     * Shuts down this invalidator and releases used resources.
     * Aimed to call with {@link ManagedService#shutdown(boolean)}
     *
     * @see ManagedService#shutdown(boolean)
     */
    public void shutdown() {
        // nop
    }
}
