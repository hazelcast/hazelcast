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

package com.hazelcast.map.impl.nearcache.invalidation;

import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataGenerator;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.spi.serialization.SerializationService;

import java.util.UUID;

import static com.hazelcast.core.EntryEventType.INVALIDATION;


/**
 * Contains common functionality of a {@code Invalidator}
 */
public abstract class Invalidator {

    protected final int partitionCount;
    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final EventService eventService;
    protected final SerializationService serializationService;
    protected final MetaDataGenerator metaDataGenerator;
    protected final IPartitionService partitionService;

    public Invalidator(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.eventService = nodeEngine.getEventService();
        this.serializationService = nodeEngine.getSerializationService();
        this.partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        this.metaDataGenerator = new MetaDataGenerator(partitionCount);
    }

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
        invalidateInternal(invalidation, orderKey);
    }

    protected abstract void invalidateInternal(Invalidation invalidation, int orderKey);

    public MetaDataGenerator getMetaDataGenerator() {
        return metaDataGenerator;
    }

    protected final Invalidation newKeyInvalidation(Data key, String mapName, String sourceUuid) {
        int partitionId = getPartitionId(key);
        long sequence = metaDataGenerator.nextSequence(mapName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        return new SingleNearCacheInvalidation(toHeapData(key), mapName, sourceUuid, partitionUuid, sequence);
    }

    protected final Invalidation newClearInvalidation(String mapName, String sourceUuid) {
        int partitionId = getPartitionId(mapName);
        long sequence = metaDataGenerator.nextSequence(mapName, partitionId);
        UUID partitionUuid = metaDataGenerator.getOrCreateUuid(partitionId);
        return new SingleNearCacheInvalidation(mapName, sourceUuid, partitionUuid, sequence);
    }

    protected final int getPartitionId(Data o) {
        return partitionService.getPartitionId(o);
    }

    protected final int getPartitionId(Object o) {
        return partitionService.getPartitionId(o);
    }

    protected final boolean canSendInvalidation(final EventFilter filter) {
        if (!(filter instanceof EventListenerFilter)) {
            return false;
        }

        if (!filter.eval(INVALIDATION.getType())) {
            return false;
        }

        return true;
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
