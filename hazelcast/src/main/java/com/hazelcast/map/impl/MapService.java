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

import com.hazelcast.core.DistributedObject;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.partition.InternalPartitionLostEvent;
import com.hazelcast.spi.ClientAwareService;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.QuorumAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.wan.WanReplicationEvent;
import java.util.Map;
import java.util.Properties;

/**
 * Defines map service behavior.
 *
 * @see MapManagedService
 * @see MapMigrationAwareService
 * @see MapTransactionalService
 * @see MapRemoteService
 * @see MapEventPublishingService
 * @see MapPostJoinAwareService
 * @see MapSplitBrainHandlerService
 * @see MapReplicationSupportingService
 * @see MapStatisticsAwareService
 * @see MapPartitionAwareService
 * @see MapQuorumAwareService
 * @see MapClientAwareService
 * @see MapServiceContext
 */
public class MapService implements ManagedService, MigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, ListenerAdapter>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService, StatisticsAwareService,
        PartitionAwareService, ClientAwareService, QuorumAwareService {

    /**
     * Service name of map service used
     * to register {@link com.hazelcast.spi.impl.ServiceManager#registerService}
     */
    public static final String SERVICE_NAME = "hz:impl:mapService";

    protected ManagedService managedService;
    protected MigrationAwareService migrationAwareService;
    protected TransactionalService transactionalService;
    protected RemoteService remoteService;
    protected EventPublishingService eventPublishingService;
    protected PostJoinAwareService postJoinAwareService;
    protected SplitBrainHandlerService splitBrainHandlerService;
    protected ReplicationSupportingService replicationSupportingService;
    protected StatisticsAwareService statisticsAwareService;
    protected PartitionAwareService partitionAwareService;
    protected ClientAwareService clientAwareService;
    protected QuorumAwareService quorumAwareService;
    protected MapServiceContext mapServiceContext;

    public MapService() {
    }

    @Override
    public void dispatchEvent(EventData event, ListenerAdapter listener) {
        eventPublishingService.dispatchEvent(event, listener);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        managedService.init(nodeEngine, properties);
    }

    @Override
    public void reset() {
        managedService.reset();
    }

    @Override
    public void shutdown(boolean terminate) {
        managedService.shutdown(terminate);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        migrationAwareService.beforeMigration(event);
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        migrationAwareService.commitMigration(event);
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        migrationAwareService.rollbackMigration(event);
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        migrationAwareService.clearPartitionReplica(partitionId);
    }

    @Override
    public Operation getPostJoinOperation() {
        return postJoinAwareService.getPostJoinOperation();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return remoteService.createDistributedObject(objectName);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        remoteService.destroyDistributedObject(objectName);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        replicationSupportingService.onReplicationEvent(replicationEvent);
    }

    @Override
    public void onPartitionLost(InternalPartitionLostEvent partitionLostEvent) {
        partitionAwareService.onPartitionLost(partitionLostEvent);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, TransactionSupport transaction) {
        return transactionalService.createTransactionalObject(name, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {
        transactionalService.rollbackTransaction(transactionId);
    }

    @Override
    public Map<String, LocalMapStats> getStats() {
        return statisticsAwareService.getStats();
    }

    @Override
    public String getQuorumName(String name) {
        return quorumAwareService.getQuorumName(name);
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        clientAwareService.clientDisconnected(clientUuid);
    }
}
