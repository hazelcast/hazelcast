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
 * An abstract implementation of {@link MapServiceContextAwareFactory} interface; this abstract class knows
 * all required auxiliary services which should be created before the construction of a new {@link MapService}.
 *
 * @see MapServiceContextAwareFactory
 */
abstract class AbstractMapServiceContextAwareFactory implements MapServiceContextAwareFactory {

    /**
     * Creates a new {@link ManagedService} for {@link MapService}.
     *
     * @return Creates a new {@link ManagedService} implementation.
     * @see com.hazelcast.spi.ManagedService
     */
    abstract ManagedService createManagedService();

    /**
     * Creates a new {@link MigrationAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link MigrationAwareService} implementation.
     * @see com.hazelcast.spi.MigrationAwareService
     */
    abstract MigrationAwareService createMigrationAwareService();

    /**
     * Creates a new {@link TransactionalService} for {@link MapService}.
     *
     * @return Creates a new {@link TransactionalService} implementation.
     * @see com.hazelcast.spi.TransactionalService
     */
    abstract TransactionalService createTransactionalService();

    /**
     * Creates a new {@link RemoteService} for {@link MapService}.
     *
     * @return Creates a new {@link RemoteService} implementation.
     * @see com.hazelcast.spi.RemoteService
     */
    abstract RemoteService createRemoteService();

    /**
     * Creates a new {@link EventPublishingService} for {@link MapService}.
     *
     * @return Creates a new {@link EventPublishingService} implementation.
     * @see com.hazelcast.spi.EventPublishingService
     */
    abstract EventPublishingService createEventPublishingService();

    /**
     * Creates a new {@link PostJoinAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link PostJoinAwareService} implementation.
     * @see com.hazelcast.spi.PostJoinAwareService
     */
    abstract PostJoinAwareService createPostJoinAwareService();

    /**
     * Creates a new {@link SplitBrainHandlerService} for {@link MapService}.
     *
     * @return Creates a new {@link SplitBrainHandlerService} implementation.
     * @see com.hazelcast.spi.SplitBrainHandlerService
     */
    abstract SplitBrainHandlerService createSplitBrainHandlerService();

    /**
     * Creates a new {@link ReplicationSupportingService} for {@link MapService}.
     *
     * @return Creates a new {@link ReplicationSupportingService} implementation.
     * @see com.hazelcast.spi.ReplicationSupportingService
     */
    abstract ReplicationSupportingService createReplicationSupportingService();

    /**
     * Creates a new {@link StatisticsAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link StatisticsAwareService} implementation.
     * @see com.hazelcast.spi.StatisticsAwareService
     */
    abstract StatisticsAwareService createStatisticsAwareService();

    /**
     * Returns a {@link MapService} object by populating it with required
     * auxiliary services.
     *
     * @return {@link MapService} object
     */
    @Override
    public MapService createMapService() {
        MapServiceContext mapServiceContext = getMapServiceContext();
        ManagedService managedService = createManagedService();
        MigrationAwareService migrationAwareService = createMigrationAwareService();
        TransactionalService transactionalService = createTransactionalService();
        RemoteService remoteService = createRemoteService();
        EventPublishingService eventPublishingService = createEventPublishingService();
        PostJoinAwareService postJoinAwareService = createPostJoinAwareService();
        SplitBrainHandlerService splitBrainHandlerService = createSplitBrainHandlerService();
        ReplicationSupportingService replicationSupportingService = createReplicationSupportingService();
        StatisticsAwareService statisticsAwareService = createStatisticsAwareService();

        checkNotNull(mapServiceContext, "mapServiceContext should not be null");
        checkNotNull(managedService, "managedService should not be null");
        checkNotNull(migrationAwareService, "migrationAwareService should not be null");
        checkNotNull(transactionalService, "transactionalService should not be null");
        checkNotNull(remoteService, "remoteService should not be null");
        checkNotNull(eventPublishingService, "eventPublishingService should not be null");
        checkNotNull(postJoinAwareService, "postJoinAwareService should not be null");
        checkNotNull(splitBrainHandlerService, "splitBrainHandlerService should not be null");
        checkNotNull(replicationSupportingService, "replicationSupportingService should not be null");
        checkNotNull(statisticsAwareService, "statisticsAwareService should not be null");

        MapService mapService = new MapService();
        mapService.setManagedService(managedService);
        mapService.setMigrationAwareService(migrationAwareService);
        mapService.setTransactionalService(transactionalService);
        mapService.setRemoteService(remoteService);
        mapService.setEventPublishingService(eventPublishingService);
        mapService.setPostJoinAwareService(postJoinAwareService);
        mapService.setSplitBrainHandlerService(splitBrainHandlerService);
        mapService.setReplicationSupportingService(replicationSupportingService);
        mapService.setStatisticsAwareService(statisticsAwareService);
        mapService.setMapServiceContext(mapServiceContext);
        mapServiceContext.setService(mapService);
        return mapService;
    }
}

