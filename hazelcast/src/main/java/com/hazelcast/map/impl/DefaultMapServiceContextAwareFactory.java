/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;

import static com.hazelcast.util.ValidationUtil.checkNotNull;

/**
 * Default implementation of {@link MapServiceContextAwareFactory}
 *
 * @see MapServiceContextAwareFactory
 */
class DefaultMapServiceContextAwareFactory extends AbstractMapServiceContextAwareFactory {

    private final MapServiceContext mapServiceContext;

    public DefaultMapServiceContextAwareFactory(MapServiceContext mapServiceContext) {
        this.mapServiceContext = checkNotNull(mapServiceContext, "mapServiceContext should not be null");
    }

    @Override
    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    ManagedService createManagedService() {
        return new MapManagedService(getMapServiceContext());
    }

    @Override
    MigrationAwareService createMigrationAwareService() {
        return new MapMigrationAwareService(getMapServiceContext());
    }

    @Override
    TransactionalService createTransactionalService() {
        return new MapTransactionalService(getMapServiceContext());
    }

    @Override
    RemoteService createRemoteService() {
        return new MapRemoteService(getMapServiceContext());
    }

    @Override
    EventPublishingService createEventPublishingService() {
        return new MapEventPublishingService(getMapServiceContext());
    }

    @Override
    PostJoinAwareService createPostJoinAwareService() {
        return new MapPostJoinAwareService(getMapServiceContext());
    }

    @Override
    SplitBrainHandlerService createSplitBrainHandlerService() {
        return new MapSplitBrainHandlerService(getMapServiceContext());
    }

    @Override
    ReplicationSupportingService createReplicationSupportingService() {
        return new MapReplicationSupportingService(getMapServiceContext());
    }

    /**
     * Creates a new {@link com.hazelcast.spi.StatisticsAwareService} for {@link com.hazelcast.map.impl.MapService}.
     *
     * @return Creates a new {@link com.hazelcast.spi.StatisticsAwareService} implementation.
     * @see com.hazelcast.spi.StatisticsAwareService
     */
    @Override
    StatisticsAwareService createStatisticsAwareService() {
        return new MapStatisticsAwareService(getMapServiceContext());
    }

}
