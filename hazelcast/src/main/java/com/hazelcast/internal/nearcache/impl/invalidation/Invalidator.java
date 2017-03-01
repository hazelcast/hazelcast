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

package com.hazelcast.internal.nearcache.impl.invalidation;

import com.hazelcast.core.IFunction;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.Collection;
import java.util.UUID;

import static java.lang.String.format;


/**
 * Invalidates near caches. Contains shared functionality.
 */
public abstract class Invalidator {

    protected final int partitionCount;
    protected final String serviceName;
    protected final ILogger logger;
    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final SerializationService serializationService;
    protected final MetaDataGenerator metaDataGenerator;
    protected final IPartitionService partitionService;
    protected final IFunction<EventRegistration, Boolean> eventFilter;

    public Invalidator(String serviceName, IFunction<EventRegistration, Boolean> eventFilter, NodeEngine nodeEngine) {
        this.serviceName = serviceName;
        this.eventFilter = eventFilter;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.partitionService = nodeEngine.getPartitionService();
        this.eventService = nodeEngine.getEventService();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.metaDataGenerator = new MetaDataGenerator(partitionCount);
    }

    protected abstract void invalidateInternal(Invalidation invalidation, int orderKey);

    /**
     * Invalidates supplied key from near-caches of supplied data structure name.
     *
     * @param key               key of the entry to be removed from near-cache
     * @param dataStructureName name of the data structure to be invalidated
     */
    public final void invalidateKey(Data key, String dataStructureName, String sourceUuid) {
        assert key != null;
        assert dataStructureName != null;

        Invalidation invalidation = newKeyInvalidation(key, dataStructureName, sourceUuid);
        invalidateInternal(invalidation, getPartitionId(key));
    }

    /**
     * Invalidates all keys from near-caches of supplied data structure name.
     *
     * @param dataStructureName name of the data structure to be cleared
     */
    public final void invalidateAllKeys(String dataStructureName, String sourceUuid) {
        assert dataStructureName != null;

        int orderKey = getPartitionId(dataStructureName);
        Invalidation invalidation = newClearInvalidation(dataStructureName, sourceUuid);
        sendImmediately(invalidation, orderKey);
    }

    public final MetaDataGenerator getMetaDataGenerator() {
        return metaDataGenerator;
    }

    private Invalidation newKeyInvalidation(Data key, String dataStructureName, String sourceUuid) {
        int partitionId = getPartitionId(key);
        long sequence = metaDataGenerator.nextSequence(dataStructureName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        if (logger.isFinestEnabled()) {
            logger.finest(format("dataStructureName=%s, partition=%d, sequence=%d, uuid=%s",
                    dataStructureName, partitionId, sequence, partitionUuid));
        }
        return new SingleNearCacheInvalidation(toHeapData(key), dataStructureName, sourceUuid, partitionUuid, sequence);
    }

    protected final Invalidation newClearInvalidation(String dataStructureName, String sourceUuid) {
        int partitionId = getPartitionId(dataStructureName);
        long sequence = metaDataGenerator.nextSequence(dataStructureName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        return new SingleNearCacheInvalidation(null, dataStructureName, sourceUuid, partitionUuid, sequence);
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

    private Data toHeapData(Data key) {
        return serializationService.toData(key);
    }

    /**
     * Removes supplied data structures invalidation queues and flushes their content.
     * This method is called when removing a Near Cache for example with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param dataStructureName name of the data structure.
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    public void destroy(String dataStructureName, String sourceUuid) {
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
     * Aimed to call with {@link com.hazelcast.spi.ManagedService#shutdown(boolean)}
     *
     * @see ManagedService#shutdown(boolean)
     */
    public void shutdown() {
        // nop
    }

}
