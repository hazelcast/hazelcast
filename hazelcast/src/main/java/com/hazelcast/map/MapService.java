/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map;

import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.PostJoinAwareService;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.spi.ReplicationSupportingService;
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.TransactionalService;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Properties;

/**
 * Defines map service behavior.
 *
 * @see com.hazelcast.map.MapManagedService
 * @see com.hazelcast.map.MapMigrationAwareService
 * @see com.hazelcast.map.MapTransactionalService
 * @see com.hazelcast.map.MapRemoteService
 * @see com.hazelcast.map.MapEventPublishingService
 * @see com.hazelcast.map.MapPostJoinAwareService
 * @see com.hazelcast.map.MapSplitBrainHandler
 * @see com.hazelcast.map.MapReplicationSupportingService
 */
public final class MapService implements ManagedService, MigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, EntryListener>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService {

    /**
     * Service name of map service used
     * to register {@link com.hazelcast.spi.impl.ServiceManager#registerService}
     */
    public static final String SERVICE_NAME = "hz:impl:mapService";

    private ManagedService managedService;
    private MigrationAwareService migrationAwareService;
    private TransactionalService transactionalService;
    private RemoteService remoteService;
    private EventPublishingService eventPublishingService;
    private PostJoinAwareService postJoinAwareService;
    private SplitBrainHandlerService splitBrainHandlerService;
    private ReplicationSupportingService replicationSupportingService;
    private MapServiceContext mapServiceContext;

    private MapService() {
    }

    @Override
    public void dispatchEvent(EventData event, EntryListener listener) {
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

    public void setMapServiceContext(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    /**
     * Static factory method which creates a new map service object.
     *
     * @param nodeEngine node engine.
     * @return new map service object.
     */
    public static MapService create(NodeEngine nodeEngine) {
        final MapServiceContext mapServiceContext = new DefaultMapServiceContext(nodeEngine);
        final ManagedService managedService = new MapManagedService(mapServiceContext);
        final MigrationAwareService migrationAwareService = new MapMigrationAwareService(mapServiceContext);
        final TransactionalService transactionalService = new MapTransactionalService(mapServiceContext);
        final RemoteService remoteService = new MapRemoteService(mapServiceContext);
        final EventPublishingService eventPublisher = new MapEventPublishingService(mapServiceContext);
        final PostJoinAwareService postJoinAwareService = new MapPostJoinAwareService(mapServiceContext);
        final SplitBrainHandlerService splitBrainHandler = new MapSplitBrainHandler(mapServiceContext);
        final ReplicationSupportingService replicationSupportingService
                = new MapReplicationSupportingService(mapServiceContext, nodeEngine);

        final MapService mapService = new MapService();
        mapService.setManagedService(managedService);
        mapService.setMigrationAwareService(migrationAwareService);
        mapService.setTransactionalService(transactionalService);
        mapService.setRemoteService(remoteService);
        mapService.setEventPublishingService(eventPublisher);
        mapService.setPostJoinAwareService(postJoinAwareService);
        mapService.setSplitBrainHandlerService(splitBrainHandler);
        mapService.setReplicationSupportingService(replicationSupportingService);
        mapService.setMapServiceContext(mapServiceContext);

        mapServiceContext.setService(mapService);

        return mapService;
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

}
