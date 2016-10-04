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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheExecutor;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.cache.impl.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapManagedService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.invalidation.BatchInvalidator;
import com.hazelcast.map.impl.nearcache.invalidation.NearCacheInvalidator;
import com.hazelcast.map.impl.nearcache.invalidation.NonStopInvalidator;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.nearcache.StaleReadPreventerNearCacheWrapper.wrapAsStaleReadPreventerNearCache;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

/**
 * Provides Near Cache specific functionality.
 */
public class NearCacheProvider {

    protected final NearCacheManager nearCacheManager;
    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final NearCacheInvalidator nearCacheInvalidator;

    public NearCacheProvider(MapServiceContext mapServiceContext) {
        this(mapServiceContext, new DefaultNearCacheManager());
    }

    protected NearCacheProvider(MapServiceContext mapServiceContext, NearCacheManager nearCacheManager) {
        this.nearCacheManager = nearCacheManager;
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.nearCacheInvalidator = isBatchingEnabled() ? new BatchInvalidator(nodeEngine) : new NonStopInvalidator(nodeEngine);
    }

    private boolean isBatchingEnabled() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        return hazelcastProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;
    }

    public <K, V> NearCache<K, V> getOrCreateNearCache(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);
        NearCacheContext nearCacheContext = new NearCacheContext(
                nodeEngine.getSerializationService(),
                new MemberNearCacheExecutor(nodeEngine.getExecutionService()),
                nearCacheManager
        );

        NearCache<K, V> nearCache = nearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig, nearCacheContext);

        int partitionCount = mapServiceContext.getNodeEngine().getPartitionService().getPartitionCount();
        return wrapAsStaleReadPreventerNearCache(nearCache, partitionCount);
    }

    private NearCacheConfig getNearCacheConfig(String mapName) {
        return nodeEngine.getConfig().getMapConfig(mapName).getNearCacheConfig();
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        nearCacheManager.clearAllNearCaches();
        nearCacheInvalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        nearCacheManager.destroyAllNearCaches();
        nearCacheInvalidator.shutdown();
    }

    /**
     * @see com.hazelcast.map.impl.MapRemoteService#destroyDistributedObject(String)
     */
    public void destroyNearCache(String mapName) {
        nearCacheManager.destroyNearCache(mapName);

        String uuid = mapServiceContext.getNodeEngine().getLocalMember().getUuid();
        nearCacheInvalidator.destroy(mapName, uuid);
    }

    public Object getFromNearCache(String mapName, Data key) {
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        if (!mapContainer.hasInvalidationListener()) {
            return null;
        }
        NearCache<Data, Object> nearCache = getOrCreateNearCache(mapName);
        return nearCache.get(key);
    }

    public NearCacheInvalidator getNearCacheInvalidator() {
        return nearCacheInvalidator;
    }

    private static final class MemberNearCacheExecutor implements NearCacheExecutor {

        private ExecutionService executionService;

        private MemberNearCacheExecutor(ExecutionService executionService) {
            this.executionService = executionService;
        }

        @Override
        public ScheduledFuture<?> scheduleWithRepetition(Runnable command, long initialDelay, long delay, TimeUnit unit) {
            return executionService.scheduleWithRepetition(command, initialDelay, delay, unit);
        }
    }
}
