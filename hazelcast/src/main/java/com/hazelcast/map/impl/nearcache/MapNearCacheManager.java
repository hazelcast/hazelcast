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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.BatchInvalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.InvalidationMetaDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.Invalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.invalidation.NonStopInvalidator;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingHandler;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.EventListenerFilter;
import com.hazelcast.map.impl.MapManagedService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.invalidation.MemberMapInvalidationMetaDataFetcher;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.UUID;
import java.util.function.Function;

import static com.hazelcast.core.EntryEventType.INVALIDATION;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;

public class MapNearCacheManager extends DefaultNearCacheManager {

    /**
     * Filters out listeners other than invalidation related ones.
     */
    private static final InvalidationAcceptorFilter INVALIDATION_ACCEPTOR = new InvalidationAcceptorFilter();

    protected final int partitionCount;
    protected final NodeEngine nodeEngine;
    protected final MapServiceContext mapServiceContext;
    protected final MinimalPartitionService partitionService;
    protected final Invalidator invalidator;
    protected final RepairingTask repairingTask;

    public MapNearCacheManager(MapServiceContext mapServiceContext) {
        super(mapServiceContext.getNodeEngine().getSerializationService(),
                mapServiceContext.getNodeEngine().getExecutionService().getGlobalTaskScheduler(),
                null, mapServiceContext.getNodeEngine().getProperties());
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.mapServiceContext = mapServiceContext;
        this.partitionService = new MemberMinimalPartitionService(nodeEngine.getPartitionService());
        this.partitionCount = partitionService.getPartitionCount();
        this.invalidator = createInvalidator();
        this.repairingTask = createRepairingInvalidationTask();
    }

    private Invalidator createInvalidator() {
        HazelcastProperties hazelcastProperties = nodeEngine.getProperties();
        int batchSize = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_SIZE);
        int batchFrequencySeconds = hazelcastProperties.getInteger(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS);
        boolean batchingEnabled = hazelcastProperties.getBoolean(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED) && batchSize > 1;

        if (batchingEnabled) {
            return new BatchInvalidator(SERVICE_NAME, batchSize, batchFrequencySeconds, INVALIDATION_ACCEPTOR, nodeEngine);
        } else {
            return new NonStopInvalidator(SERVICE_NAME, INVALIDATION_ACCEPTOR, nodeEngine);
        }
    }

    /**
     * Filters out listeners other than invalidation related ones.
     */
    private static class InvalidationAcceptorFilter implements Function<EventRegistration, Boolean> {

        @Override
        public Boolean apply(EventRegistration eventRegistration) {
            EventFilter filter = eventRegistration.getFilter();
            return filter instanceof EventListenerFilter && filter.eval(INVALIDATION.getType());
        }
    }

    private RepairingTask createRepairingInvalidationTask() {
        ExecutionService executionService = nodeEngine.getExecutionService();
        ClusterService clusterService = nodeEngine.getClusterService();
        OperationService operationService = nodeEngine.getOperationService();
        HazelcastProperties properties = nodeEngine.getProperties();

        ILogger metadataFetcherLogger = nodeEngine.getLogger(MemberMapInvalidationMetaDataFetcher.class);
        InvalidationMetaDataFetcher invalidationMetaDataFetcher
                = new MemberMapInvalidationMetaDataFetcher(clusterService, operationService, metadataFetcherLogger);

        ILogger repairingTaskLogger = nodeEngine.getLogger(RepairingTask.class);
        UUID localUuid = nodeEngine.getLocalMember().getUuid();
        return new RepairingTask(properties, invalidationMetaDataFetcher, executionService.getGlobalTaskScheduler(),
                serializationService, partitionService, localUuid, repairingTaskLogger);
    }

    /**
     * @see MapManagedService#reset()
     */
    public void reset() {
        clearAllNearCaches();
        invalidator.reset();
    }

    /**
     * @see MapManagedService#shutdown(boolean)
     */
    public void shutdown() {
        destroyAllNearCaches();
        invalidator.shutdown();
    }

    /**
     * @see RemoteService#destroyDistributedObject(String)
     */
    @Override
    public boolean destroyNearCache(String mapName) {
        invalidator.destroy(mapName, nodeEngine.getLocalMember().getUuid());
        return super.destroyNearCache(mapName);
    }

    public Invalidator getInvalidator() {
        return invalidator;
    }

    public RepairingHandler newRepairingHandler(String name, NearCache nearCache) {
        return repairingTask.registerAndGetHandler(name, nearCache);
    }

    public void deregisterRepairingHandler(String name) {
        repairingTask.deregisterHandler(name);
    }

    // used in tests
    public RepairingTask getRepairingTask() {
        return repairingTask;
    }
}
