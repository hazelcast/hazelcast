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

import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.WanSupportingService;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.CountingMigrationAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.internal.partition.MigrationAwareService;
import com.hazelcast.internal.partition.PartitionAwareService;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * An abstract implementation of {@link MapServiceFactory} interface; this abstract class knows
 * all required auxiliary services which should be created before the construction of a new {@link MapService}.
 *
 * @see MapServiceFactory
 */
abstract class AbstractMapServiceFactory implements MapServiceFactory {

    /**
     * Creates a new {@link ManagedService} for {@link MapService}.
     *
     * @return Creates a new {@link ManagedService} implementation.
     * @see ManagedService
     */
    abstract ManagedService createManagedService();

    /**
     * Creates a new {@link MigrationAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link MigrationAwareService} implementation.
     * @see MigrationAwareService
     */
    abstract CountingMigrationAwareService createMigrationAwareService();

    /**
     * Creates a new {@link TransactionalService} for {@link MapService}.
     *
     * @return Creates a new {@link TransactionalService} implementation.
     * @see TransactionalService
     */
    abstract TransactionalService createTransactionalService();

    /**
     * Creates a new {@link RemoteService} for {@link MapService}.
     *
     * @return Creates a new {@link RemoteService} implementation.
     * @see RemoteService
     */
    abstract RemoteService createRemoteService();

    /**
     * Creates a new {@link EventPublishingService} for {@link MapService}.
     *
     * @return Creates a new {@link EventPublishingService} implementation.
     * @see EventPublishingService
     */
    abstract EventPublishingService createEventPublishingService();

    /**
     * Creates a new {@link PostJoinAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link PostJoinAwareService} implementation.
     * @see PostJoinAwareService
     */
    abstract PostJoinAwareService createPostJoinAwareService();

    /**
     * Creates a new {@link SplitBrainHandlerService} for {@link MapService}.
     *
     * @return Creates a new {@link SplitBrainHandlerService} implementation.
     * @see SplitBrainHandlerService
     */
    abstract SplitBrainHandlerService createSplitBrainHandlerService();

    /**
     * Creates a new {@link WanSupportingService} for {@link MapService}.
     *
     * @return Creates a new {@link WanSupportingService} implementation.
     * @see WanSupportingService
     */
    abstract WanSupportingService createReplicationSupportingService();

    /**
     * Creates a new {@link StatisticsAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link StatisticsAwareService} implementation.
     * @see StatisticsAwareService
     */
    abstract StatisticsAwareService createStatisticsAwareService();

    /**
     * Creates a new {@link PartitionAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link PartitionAwareService} implementation.
     * @see PartitionAwareService
     */
    abstract PartitionAwareService createPartitionAwareService();


    /**
     * Creates a new {@link ClientAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link ClientAwareService} implementation.
     * @see ClientAwareService
     */
    abstract ClientAwareService createClientAwareService();

    /**
     * Creates a new {@link SplitBrainProtectionAwareService} for {@link MapService}.
     *
     * @return Creates a new {@link PartitionAwareService} implementation.
     * @see PartitionAwareService
     */
    abstract MapSplitBrainProtectionAwareService createSplitBrainProtectionAwareService();


    /**
     * Returns a {@link MapService} object by populating it with required
     * auxiliary services.
     *
     * @return {@link MapService} object
     */
    @Override
    public MapService createMapService() {
        NodeEngine nodeEngine = getNodeEngine();
        MapServiceContext mapServiceContext = getMapServiceContext();
        ManagedService managedService = createManagedService();
        CountingMigrationAwareService migrationAwareService = createMigrationAwareService();
        TransactionalService transactionalService = createTransactionalService();
        RemoteService remoteService = createRemoteService();
        EventPublishingService eventPublishingService = createEventPublishingService();
        PostJoinAwareService postJoinAwareService = createPostJoinAwareService();
        SplitBrainHandlerService splitBrainHandlerService = createSplitBrainHandlerService();
        WanSupportingService wanSupportingService = createReplicationSupportingService();
        StatisticsAwareService statisticsAwareService = createStatisticsAwareService();
        PartitionAwareService partitionAwareService = createPartitionAwareService();
        MapSplitBrainProtectionAwareService splitBrainProtectionAwareService =
                createSplitBrainProtectionAwareService();
        ClientAwareService clientAwareService = createClientAwareService();

        checkNotNull(nodeEngine, "nodeEngine should not be null");
        checkNotNull(mapServiceContext, "mapServiceContext should not be null");
        checkNotNull(managedService, "managedService should not be null");
        checkNotNull(migrationAwareService, "migrationAwareService should not be null");
        checkNotNull(transactionalService, "transactionalService should not be null");
        checkNotNull(remoteService, "remoteService should not be null");
        checkNotNull(eventPublishingService, "eventPublishingService should not be null");
        checkNotNull(postJoinAwareService, "postJoinAwareService should not be null");
        checkNotNull(splitBrainHandlerService, "splitBrainHandlerService should not be null");
        checkNotNull(wanSupportingService, "replicationSupportingService should not be null");
        checkNotNull(statisticsAwareService, "statisticsAwareService should not be null");
        checkNotNull(partitionAwareService, "partitionAwareService should not be null");
        checkNotNull(splitBrainProtectionAwareService, "splitBrainProtectionAwareService should not be null");
        checkNotNull(clientAwareService, "clientAwareService should not be null");

        MapService mapService = new MapService();
        mapService.managedService = managedService;
        mapService.migrationAwareService = migrationAwareService;
        mapService.transactionalService = transactionalService;
        mapService.remoteService = remoteService;
        mapService.eventPublishingService = eventPublishingService;
        mapService.postJoinAwareService = postJoinAwareService;
        mapService.splitBrainHandlerService = splitBrainHandlerService;
        mapService.wanSupportingService = wanSupportingService;
        mapService.statisticsAwareService = statisticsAwareService;
        mapService.mapServiceContext = mapServiceContext;
        mapService.partitionAwareService = partitionAwareService;
        mapService.splitBrainProtectionAwareService = splitBrainProtectionAwareService;
        mapService.clientAwareService = clientAwareService;
        mapServiceContext.setService(mapService);
        return mapService;
    }
}

