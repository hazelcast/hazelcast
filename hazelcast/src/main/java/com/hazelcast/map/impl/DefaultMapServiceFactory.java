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

package com.hazelcast.map.impl;

import com.hazelcast.map.impl.event.MapEventPublishingService;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.spi.impl.CountingMigrationAwareService;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link MapServiceFactory}
 *
 * @see MapServiceFactory
 */
class DefaultMapServiceFactory extends AbstractMapServiceFactory {

    private final NodeEngine nodeEngine;
    private final MapServiceContext mapServiceContext;

    public DefaultMapServiceFactory(NodeEngine nodeEngine, MapServiceContext mapServiceContext) {
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
    ReplicationSupportingService createReplicationSupportingService() {
        return new MapReplicationSupportingService(mapServiceContext);
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
    QuorumAwareService createQuorumAwareService() {
        return new MapQuorumAwareService(getMapServiceContext());
    }

    @Override
    ClientAwareService createClientAwareService() {
        return new MapClientAwareService(getMapServiceContext());
    }

}
