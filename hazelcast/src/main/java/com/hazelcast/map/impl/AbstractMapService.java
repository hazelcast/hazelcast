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
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;

/**
 * Base class contains service definitions and initialization methods for {@link com.hazelcast.map.impl.MapService}
 */
public abstract class AbstractMapService implements ManagedService, MigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, ListenerAdapter>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService, StatisticsAwareService,
        PartitionAwareService, QuorumAwareService {

    protected ManagedService managedService;
    protected MigrationAwareService migrationAwareService;
    protected TransactionalService transactionalService;
    protected RemoteService remoteService;
    protected EventPublishingService eventPublishingService;
    protected PostJoinAwareService postJoinAwareService;
    protected SplitBrainHandlerService splitBrainHandlerService;
    protected ReplicationSupportingService replicationSupportingService;
    protected StatisticsAwareService statisticsAwareService;
    protected PartitionAwareService mapPartitionAwareService;
    protected QuorumAwareService mapQuorumAwareService;
    protected MapServiceContext mapServiceContext;

    public void setMapServiceContext(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    void setManagedService(ManagedService managedService) {
        this.managedService = managedService;
    }

    void setMigrationAwareService(MigrationAwareService migrationAwareService) {
        this.migrationAwareService = migrationAwareService;
    }

    void setTransactionalService(TransactionalService transactionalService) {
        this.transactionalService = transactionalService;
    }

    void setRemoteService(RemoteService remoteService) {
        this.remoteService = remoteService;
    }

    void setEventPublishingService(EventPublishingService eventPublishingService) {
        this.eventPublishingService = eventPublishingService;
    }

    void setPostJoinAwareService(PostJoinAwareService postJoinAwareService) {
        this.postJoinAwareService = postJoinAwareService;
    }

    void setSplitBrainHandlerService(SplitBrainHandlerService splitBrainHandlerService) {
        this.splitBrainHandlerService = splitBrainHandlerService;
    }

    void setReplicationSupportingService(ReplicationSupportingService replicationSupportingService) {
        this.replicationSupportingService = replicationSupportingService;
    }

    void setStatisticsAwareService(StatisticsAwareService statisticsAwareService) {
        this.statisticsAwareService = statisticsAwareService;
    }

    public void setMapPartitionAwareService(PartitionAwareService mapPartitionAwareService) {
        this.mapPartitionAwareService = mapPartitionAwareService;
    }

    public void setMapQuorumAwareService(QuorumAwareService quorumAwareService) {
        this.mapQuorumAwareService = quorumAwareService;
    }
}
