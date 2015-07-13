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

import com.hazelcast.cluster.impl.ClusterServiceImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.monitor.LocalReplicatedMapStats;
import com.hazelcast.monitor.impl.LocalReplicatedMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.replicatedmap.impl.operation.CheckReplicaVersion;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapClearOperation;
import com.hazelcast.replicatedmap.impl.operation.ReplicationOperation;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.replicatedmap.merge.MergePolicyProvider;
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
import com.hazelcast.spi.SplitBrainHandlerService;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.MapUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EventListener;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * This is the main service implementation to handle proxy creation, event publishing, migration, anti-entropy and
 * manages the backing {@link PartitionContainer}s that actually hold the data
 */
public class ReplicatedMapService implements ManagedService, RemoteService, EventPublishingService<Object, Object>,
        MigrationAwareService, SplitBrainHandlerService, StatisticsAwareService {

    /**
     * Public constant for the internal service name of the ReplicatedMapService
     */
    public static final String SERVICE_NAME = "hz:impl:replicatedMapService";

    private static final int SYNC_INTERVAL_SECONDS = 10;
    private static final int MAX_CLEAR_EXECUTION_RETRY = 5;

    private final Config config;
    private final NodeEngine nodeEngine;
    private final EventService eventService;
    private final PartitionContainer[] partitionContainers;
    private final InternalPartitionServiceImpl partitionService;
    private final ClusterServiceImpl clusterService;
    private final OperationService operationService;
    private final ReplicatedMapEventPublishingService replicatedMapEventPublishingService;
    private final MergePolicyProvider mergePolicyProvider;
    private final ReplicatedMapSplitBrainHandlerService replicatedMapSplitBrainHandlerService;
    private ConcurrentHashMap<String, LocalReplicatedMapStatsImpl> statsMap =
            new ConcurrentHashMap<String, LocalReplicatedMapStatsImpl>();
    private ConstructorFunction<String, LocalReplicatedMapStatsImpl> constructorFunction =
            new ConstructorFunction<String, LocalReplicatedMapStatsImpl>() {
                @Override
                public LocalReplicatedMapStatsImpl createNew(String arg) {
                    return new LocalReplicatedMapStatsImpl();
                }
            };

    public ReplicatedMapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.config = nodeEngine.getConfig();
        this.eventService = nodeEngine.getEventService();
        this.partitionService = (InternalPartitionServiceImpl) nodeEngine.getPartitionService();
        this.clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();
        this.operationService = nodeEngine.getOperationService();
        this.partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        this.replicatedMapEventPublishingService = new ReplicatedMapEventPublishingService(this);
        this.mergePolicyProvider = new MergePolicyProvider(nodeEngine);
        this.replicatedMapSplitBrainHandlerService = new ReplicatedMapSplitBrainHandlerService(this,
                mergePolicyProvider, partitionContainers);
    }

    @Override
    public void init(final NodeEngine nodeEngine, Properties properties) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        nodeEngine.getExecutionService().getDefaultScheduledExecutor().scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (clusterService.getSize() == 1) {
                    return;
                }
                Collection<Address> addresses = new ArrayList<Address>(clusterService.getMemberAddresses());
                addresses.remove(nodeEngine.getThisAddress());
                for (int i = 0; i < partitionContainers.length; i++) {
                    Address thisAddress = nodeEngine.getThisAddress();
                    Address ownerAddress = partitionService.getPartitionOwner(i);
                    if (!thisAddress.equals(ownerAddress)) {
                        continue;
                    }
                    PartitionContainer partitionContainer = partitionContainers[i];
                    for (Address address : addresses) {
                        CheckReplicaVersion checkReplicaVersion = new CheckReplicaVersion(partitionContainer);
                        checkReplicaVersion.setPartitionId(i);
                        checkReplicaVersion.setValidateTarget(false);
                        operationService.invokeOnTarget(SERVICE_NAME, checkReplicaVersion, address);
                    }
                }
            }
        }, 0, SYNC_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ConcurrentHashMap<String, ReplicatedRecordStore> stores = partitionContainers[i].getStores();
            for (ReplicatedRecordStore store : stores.values()) {
                store.reset();
            }
        }
    }

    @Override
    public void shutdown(boolean terminate) {
        for (PartitionContainer container : partitionContainers) {
            container.shutdown();
        }
    }

    public LocalReplicatedMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, constructorFunction);
    }

    public LocalReplicatedMapStats createReplicatedMapStats(String name) {
        LocalReplicatedMapStatsImpl stats = getLocalMapStatsImpl(name);
        long hits = 0;
        long count = 0;
        for (PartitionContainer container : partitionContainers) {
            ReplicatedRecordStore store = container.getRecordStore(name);
            if (store == null) {
                continue;
            }
            Iterator<ReplicatedRecord> iterator = store.recordIterator();
            while (iterator.hasNext()) {
                ReplicatedRecord record = iterator.next();
                stats.setLastAccessTime(Math.max(stats.getLastAccessTime(), record.getLastAccessTime()));
                stats.setLastUpdateTime(Math.max(stats.getLastUpdateTime(), record.getUpdateTime()));
                hits += record.getHits();
                count++;
            }
        }
        stats.setOwnedEntryCount(count);
        stats.setHits(hits);
        return stats;
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

    public void clearLocalRecordStores(String name) {
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            ReplicatedRecordStore recordStore = partitionContainers[i].getRecordStore(name);
            if (recordStore != null) {
                recordStore.clear();
            }
        }
    }

    public void clearLocalAndRemoteRecordStores(String name) {
        Collection<Address> failedMembers = new ArrayList<Address>(clusterService.getMemberAddresses());
        for (int i = 0; i < MAX_CLEAR_EXECUTION_RETRY; i++) {
            Map<Address, InternalCompletableFuture> futures = executeClearOnMembers(failedMembers, name);
            // Clear to collect new failing members
            failedMembers.clear();
            for (Map.Entry<Address, InternalCompletableFuture> future : futures.entrySet()) {
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
        throw new OperationTimeoutException("Clear operation couldn't be finished, failed nodes: " + failedMembers);
    }

    private Map executeClearOnMembers(Collection<Address> members, String name) {
        Map<Address, InternalCompletableFuture> futures = new HashMap<Address, InternalCompletableFuture>(members.size());
        for (Address address : members) {
            Operation operation = new ReplicatedMapClearOperation(name);
            InvocationBuilder ib = operationService.createInvocationBuilder(SERVICE_NAME, operation, address);
            futures.put(address, ib.invoke());
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

    @Override
    public Runnable prepareMergeRunnable() {
        return replicatedMapSplitBrainHandlerService.prepareMergeRunnable();
    }

    @Override
    public Map<String, LocalReplicatedMapStats> getStats() {
        Collection<String> maps = getNodeEngine().getProxyService().getDistributedObjectNames(SERVICE_NAME);
        Map<String, LocalReplicatedMapStats> mapStats = MapUtil.createHashMap(maps.size());
        for (String map : maps) {
            mapStats.put(map, createReplicatedMapStats(map));
        }
        return mapStats;
    }

}
