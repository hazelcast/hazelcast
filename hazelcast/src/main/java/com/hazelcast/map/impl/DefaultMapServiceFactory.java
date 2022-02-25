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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.event.MapEventPublishingService;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.spi.impl.CountingMigrationAwareService;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MapServiceFactory}
 *
 * @see MapServiceFactory
 */
class DefaultMapServiceFactory extends AbstractMapServiceFactory {

    private final NodeEngine nodeEngine;
    private final MapServiceContext mapServiceContext;

    DefaultMapServiceFactory(NodeEngine nodeEngine, MapServiceContext mapServiceContext) {
        this.nodeEngine = checkNotNull(nodeEngine, "nodeEngine should not be null");
        this.mapServiceContext = checkNotNull(mapServiceContext, "mapServiceContext should not be null");
    }

    @Override
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    ManagedService createManagedService() {
        return new MapManagedService(mapServiceContext);
    }

    @Override
    CountingMigrationAwareService createMigrationAwareService() {
        return new CountingMigrationAwareService(new MapMigrationAwareService(mapServiceContext));
    }

    @Override
    TransactionalService createTransactionalService() {
        return new MapTransactionalService(mapServiceContext);
    }

    @Override
    RemoteService createRemoteService() {
        return new MapRemoteService(mapServiceContext);
    }

    @Override
    EventPublishingService createEventPublishingService() {
        return new MapEventPublishingService(mapServiceContext);
    }

    @Override
    PostJoinAwareService createPostJoinAwareService() {
        return new MapPostJoinAwareService(mapServiceContext);
    }

    @Override
    SplitBrainHandlerService createSplitBrainHandlerService() {
        return new MapSplitBrainHandlerService(mapServiceContext);
    }

    @Override
    WanSupportingService createReplicationSupportingService() {
        return new WanMapSupportingService(mapServiceContext);
    }

    @Override
    StatisticsAwareService createStatisticsAwareService() {
        return new MapStatisticsAwareService(mapServiceContext);
    }

    @Override
    MapPartitionAwareService createPartitionAwareService() {
        return new MapPartitionAwareService(mapServiceContext);
    }

    @Override
    MapSplitBrainProtectionAwareService createSplitBrainProtectionAwareService() {
        return new MapSplitBrainProtectionAwareService(getMapServiceContext());
    }

    @Override
    ClientAwareService createClientAwareService() {
        return new MapClientAwareService(getMapServiceContext());
    }

}
