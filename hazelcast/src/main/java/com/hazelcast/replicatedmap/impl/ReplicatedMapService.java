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

package com.hazelcast.replicatedmap.impl;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.replicatedmap.impl.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.impl.operation.CheckReplicaVersion;
import com.hazelcast.replicatedmap.impl.operation.ProcessReplicationMessageOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapClearOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicationOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.EventFilter;
import com.hazelcast.spi.EventPublishingService;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.MigrationAwareService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.PartitionMigrationEvent;
import com.hazelcast.spi.PartitionReplicationEvent;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ExceptionUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This is the main service implementation to handle replication and manages the backing
 * {@link com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore}s that actually hold the data
 */
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
        MigrationAwareService {

    /**
     * Public constant for the internal service name of the ReplicatedMapService
     */
    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";

    /**
     * Public constant for the internal name of the replication topic
     */
    public static final String EVENT_TOPIC_NAME = SERVICE_NAME + ".replication";

    private static final int SYNC_INTERVAL_SECONDS = 30;
    private static final int MAX_CLEAR_EXECUTION_RETRY = 5;

    private final Config config;
    private final NodeEngine nodeEngine;
    private final EventService eventService;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionService partitionService;
    private final ClusterService clusterService;
    private final OperationService operationService;
    private final ReplicatedMapEventPublishingService replicatedMapEventPublishingService;

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.partitionService = nodeEngine.getPartitionService();
        this.clusterService = nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.eventService.registerListener(SERVICE_NAME, EVENT_TOPIC_NAME, new ReplicationListener());
        this.replicatedMapEventPublishingService = new ReplicatedMapEventPublishingService(this);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        nodeEngine.getExecutionService().getDefaultScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < partitionContainers.length; i++) {
                    InternalPartition partition = partitionService.getPartition(i);
                    if (partition.isLocal()) {
                        continue;
                    }
                    PartitionContainer partitionContainer = partitionContainers[i];
                    ConcurrentHashMap<String, ReplicatedRecordStore> stores = partitionContainer.getStores();
                    for (Map.Entry<String, ReplicatedRecordStore> storeEntry : stores.entrySet()) {
                        String name = storeEntry.getKey();
                        ReplicatedRecordStore store = storeEntry.getValue();
                        long version = store.getPartitionVersion();
                        operationService.invokeOnPartition(SERVICE_NAME, new CheckReplicaVersion(name, version), i);
                    }
                }
            }
        }, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {
        // Nothing to do, it is ok for a ReplicatedMap to keep its current state on rejoins
    }

    @Override
    public void shutdown(boolean terminate) {
        for (PartitionContainer container : partitionContainers) {
            container.shutdown();
        }
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].getOrCreateRecordStore(objectName);
        }
        return new ReplicatedMapProxy(nodeEngine, objectName, this);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i].destroy(objectName);
        }
    }

    @Override
    public void dispatchEvent(Object event, Object listener) {
        replicatedMapEventPublishingService.dispatchEvent(event, listener);
    }


    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return config.getReplicatedMapConfig(name).getAsReadOnly();
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, Object key) {
        return getReplicatedRecordStore(name, create, partitionService.getPartitionId(key));
    }

    public ReplicatedRecordStore getReplicatedRecordStore(String name, boolean create, int partitionId) {
        PartitionContainer partitionContainer = partitionContainers[partitionId];
        if (create) {
            return partitionContainer.getOrCreateRecordStore(name);
        }
        return partitionContainer.getRecordStore(name);
    }

    public Collection<ReplicatedRecordStore> getAllReplicatedRecordStores(String name) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        ArrayList<ReplicatedRecordStore> stores = new ArrayList<ReplicatedRecordStore>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            ReplicatedRecordStore recordStore = partitionContainers[i].getRecordStore(name);
            if (recordStore == null) {
                continue;
            }
            stores.add(recordStore);
        }
        return stores;
    }

    public void clearLocalRecordStores(String name, boolean emptyReplicationQueue) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ReplicatedRecordStore recordStore = partitionContainers[i].getRecordStore(name);
            if (recordStore != null) {
                recordStore.clear(emptyReplicationQueue);
            }
        }
    }


    public void clearLocalAndRemoteRecordStores(String name, boolean emptyReplicationQueue) {
        Collection<MemberImpl> failedMembers = clusterService.getMemberImpls();
        for (int i = 0; i < MAX_CLEAR_EXECUTION_RETRY; i++) {
            Map<MemberImpl, InternalCompletableFuture> futures =
                    executeClearOnMembers(failedMembers, emptyReplicationQueue, name);
            // Clear to collect new failing members
            failedMembers.clear();
            for (Map.Entry<MemberImpl, InternalCompletableFuture> future : futures.entrySet()) {
                try {
                    future.getValue().get();
                } catch (Exception e) {
                    nodeEngine.getLogger(ReplicatedMapService.class).finest(e);
                    failedMembers.add(future.getKey());
                }
            }

            if (failedMembers.size() == 0) {
                return;
            }
        }
        // If we get here we does not seem to have finished the operation
        throw new OperationTimeoutException("ReplicatedMap::clear couldn't be finished, failed nodes: "
                + failedMembers);
    }

    private Map executeClearOnMembers(Collection<MemberImpl> members, boolean emptyReplicationQueue, String name) {
        Map<MemberImpl, InternalCompletableFuture> futures = new HashMap<MemberImpl, InternalCompletableFuture>(members.size());
        for (MemberImpl member : members) {
            Address address = member.getAddress();
            Operation operation = new ReplicatedMapClearOperation(name, emptyReplicationQueue);
            InvocationBuilder ib = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            futures.put(member, ib.invoke());
        }
        return futures;
    }

    public void initializeListeners(String name) {
        List<ListenerConfig> listenerConfigs = config.getReplicatedMapConfig(name).getListenerConfigs();
        for (ListenerConfig listenerConfig : listenerConfigs) {
            EntryListener listener = null;
            if (listenerConfig.getImplementation() != null) {
                listener = (EntryListener) listenerConfig.getImplementation();
            } else if (listenerConfig.getClassName() != null) {
                try {
                    listener = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(),
                            listenerConfig.getClassName());
                } catch (Exception e) {
                    throw ExceptionUtil.rethrow(e);
                }
            }
            if (listener != null) {
                if (listener instanceof HazelcastInstanceAware) {
                    ((HazelcastInstanceAware) listener).setHazelcastInstance(nodeEngine.getHazelcastInstance());
                }
                addEventListener(listener, null, name);
            }
        }
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public Config getConfig() {
        return config;
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public String addEventListener(EventListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = eventService.registerLocalListener(SERVICE_NAME, mapName, eventFilter,
                entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        if (registrationId == null) {
            throw new IllegalArgumentException("registrationId cannot be null");
        }
        return eventService.deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    @Override
    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        final ReplicationOperation operation = new ReplicationOperation(nodeEngine.getSerializationService(),
                container, event.getPartitionId());
        operation.setService(this);
        return operation.isEmpty() ? null : operation;
    }

    @Override
    public void beforeMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void commitMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void rollbackMigration(PartitionMigrationEvent event) {
        // no-op
    }

    @Override
    public void clearPartitionReplica(int partitionId) {
        // no-op
    }

    /**
     * Listener implementation to listen on replication messages from other nodes
     */
    private final class ReplicationListener implements ReplicatedMessageListener {

        public void onMessage(IdentifiedDataSerializable message) {
            ReplicationMessage replicationMessage = (ReplicationMessage) message;
            int partitionId = partitionService.getPartitionId(replicationMessage.getDataKey());
            ReplicatedRecordStore replicatedRecordStorage = partitionContainers[partitionId]
                    .getOrCreateRecordStore(replicationMessage.getName());
            ProcessReplicationMessageOperation op = new ProcessReplicationMessageOperation(replicatedRecordStorage,
                    replicationMessage);
            op.setNodeEngine(nodeEngine);
            op.setPartitionId(partitionId);
            op.setValidateTarget(false);
            nodeEngine.getOperationService().executeOperation(op);
        }
    }
}
