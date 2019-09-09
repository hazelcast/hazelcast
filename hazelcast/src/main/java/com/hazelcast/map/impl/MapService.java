/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.DistributedObjectNamespace;
import com.hazelcast.internal.services.LockInterceptorService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.NotifiableEventListener;
import com.hazelcast.internal.services.ObjectNamespace;
import com.hazelcast.internal.services.PostJoinAwareService;
import com.hazelcast.internal.services.RemoteService;
import com.hazelcast.internal.services.ReplicationSupportingService;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.SplitBrainHandlerService;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.map.impl.event.MapEventPublishingService;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.services.SplitBrainProtectionAwareService;
import com.hazelcast.spi.impl.CountingMigrationAwareService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.eventservice.EventFilter;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.partition.FragmentedMigrationAwareService;
import com.hazelcast.spi.partition.IPartitionLostEvent;
import com.hazelcast.spi.partition.PartitionAwareService;
import com.hazelcast.spi.partition.PartitionMigrationEvent;
import com.hazelcast.spi.partition.PartitionReplicationEvent;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.core.EntryEventType.INVALIDATION;

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
 * @see MapSplitBrainProtectionAwareService
 * @see MapClientAwareService
 * @see MapServiceContext
 */
public class MapService implements ManagedService, FragmentedMigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<Object, ListenerAdapter>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService, StatisticsAwareService<LocalMapStats>,
        PartitionAwareService, ClientAwareService, SplitBrainProtectionAwareService, NotifiableEventListener,
        ClusterStateListener, LockInterceptorService<Data> {

    public static final String SERVICE_NAME = "hz:impl:mapService";

    protected ManagedService managedService;
    protected CountingMigrationAwareService migrationAwareService;
    protected TransactionalService transactionalService;
    protected RemoteService remoteService;
    protected EventPublishingService eventPublishingService;
    protected PostJoinAwareService postJoinAwareService;
    protected SplitBrainHandlerService splitBrainHandlerService;
    protected ReplicationSupportingService replicationSupportingService;
    protected StatisticsAwareService statisticsAwareService;
    protected PartitionAwareService partitionAwareService;
    protected ClientAwareService clientAwareService;
    protected MapSplitBrainProtectionAwareService splitBrainProtectionAwareService;
    protected MapServiceContext mapServiceContext;

    public MapService() {
    }

    @Override
    public void dispatchEvent(Object event, ListenerAdapter listener) {
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
    public Collection<ServiceNamespace> getAllServiceNamespaces(PartitionReplicationEvent event) {
        return migrationAwareService.getAllServiceNamespaces(event);
    }

    @Override
    public boolean isKnownServiceNamespace(ServiceNamespace namespace) {
        return migrationAwareService.isKnownServiceNamespace(namespace);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        return migrationAwareService.prepareReplicationOperation(event);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event,
            Collection<ServiceNamespace> namespaces) {
        return migrationAwareService.prepareReplicationOperation(event, namespaces);
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
        splitBrainProtectionAwareService.onDestroy(objectName);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent event, WanAcknowledgeType acknowledgeType) {
        replicationSupportingService.onReplicationEvent(event, acknowledgeType);
    }

    @Override
    public void onPartitionLost(IPartitionLostEvent partitionLostEvent) {
        partitionAwareService.onPartitionLost(partitionLostEvent);
    }

    @Override
    public Runnable prepareMergeRunnable() {
        return splitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(String name, Transaction transaction) {
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
    public String getSplitBrainProtectionName(String name) {
        return splitBrainProtectionAwareService.getSplitBrainProtectionName(name);
    }

    public MapServiceContext getMapServiceContext() {
        return mapServiceContext;
    }

    @Override
    public void clientDisconnected(String clientUuid) {
        clientAwareService.clientDisconnected(clientUuid);
    }

    @Override
    public void onRegister(Object service, String serviceName, String topic, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(topic);
        mapContainer.increaseInvalidationListenerCount();
    }

    @Override
    public void onDeregister(Object service, String serviceName, String topic, EventRegistration registration) {
        EventFilter filter = registration.getFilter();
        if (!(filter instanceof EventListenerFilter) || !filter.eval(INVALIDATION.getType())) {
            return;
        }

        MapContainer mapContainer = mapServiceContext.getMapContainer(topic);
        mapContainer.decreaseInvalidationListenerCount();
    }

    public int getMigrationStamp() {
        return migrationAwareService.getMigrationStamp();
    }

    public boolean validateMigrationStamp(int stamp) {
        return migrationAwareService.validateMigrationStamp(stamp);
    }

    @Override
    public void onClusterStateChange(ClusterState newState) {
        mapServiceContext.onClusterStateChange(newState);
    }

    @Override
    public void onBeforeLock(String distributedObjectName, Data key) {
        int partitionId = mapServiceContext.getNodeEngine().getPartitionService().getPartitionId(key);
        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, distributedObjectName);
        // we have no use for the return value, invoked just for the side-effects
        recordStore.getRecordOrNull(key);
    }

    public static ObjectNamespace getObjectNamespace(String mapName) {
        return new DistributedObjectNamespace(SERVICE_NAME, mapName);
    }
}
