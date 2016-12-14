/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
 * Contains common functionality of a {@code Invalidator}
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
     * Invalidates supplied key from near-caches of supplied map name.
     *
     * @param key     key of the entry to be removed from near-cache
     * @param mapName name of the map to be invalidated
     */
    public final void invalidateKey(Data key, String mapName, String sourceUuid) {
        assert key != null;
        assert mapName != null;

        Invalidation invalidation = newKeyInvalidation(key, mapName, sourceUuid);
        invalidateInternal(invalidation, getPartitionId(key));
    }

    /**
     * Invalidates all keys from near-caches of supplied map name.
     *
     * @param mapName name of the map to be cleared
     */
    public final void invalidateAllKeys(String mapName, String sourceUuid) {
        assert mapName != null;

        int orderKey = getPartitionId(mapName);
        Invalidation invalidation = newClearInvalidation(mapName, sourceUuid);
        sendImmediately(invalidation, orderKey);
    }

    public final MetaDataGenerator getMetaDataGenerator() {
        return metaDataGenerator;
    }

    private Invalidation newKeyInvalidation(Data key, String mapName, String sourceUuid) {
        int partitionId = getPartitionId(key);
        long sequence = metaDataGenerator.nextSequence(mapName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        if (logger.isFinestEnabled()) {
            logger.finest(format("mapName=%s, partition=%d, sequence=%d, uuid=%s",
                    mapName, partitionId, sequence, partitionUuid));
        }
        return new SingleNearCacheInvalidation(toHeapData(key), mapName, sourceUuid, partitionUuid, sequence);
    }

    protected final Invalidation newClearInvalidation(String mapName, String sourceUuid) {
        int partitionId = getPartitionId(mapName);
        long sequence = metaDataGenerator.nextSequence(mapName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        return new SingleNearCacheInvalidation(null, mapName, sourceUuid, partitionUuid, sequence);
    }

    private int getPartitionId(Data o) {
        return partitionService.getPartitionId(o);
    }

    private int getPartitionId(Object o) {
        return partitionService.getPartitionId(o);
    }

    protected final void sendImmediately(Invalidation invalidation, int orderKey) {
        String mapName = invalidation.getName();

        Collection<EventRegistration> registrations = eventService.getRegistrations(serviceName, mapName);
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
     * Removes supplied maps invalidation queue and flushes its content.
     * This method is called when removing a Near Cache with
     * {@link com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)}
     *
     * @param mapName name of the map.
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    public void destroy(String mapName, String sourceUuid) {
        // nop.
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
