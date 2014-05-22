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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.concurrent.lock.LockService;
import com.hazelcast.concurrent.lock.LockStoreInfo;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.*;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.merge.*;
import com.hazelcast.map.operation.*;
import com.hazelcast.map.proxy.MapProxyImpl;
import com.hazelcast.map.record.Record;
import com.hazelcast.map.record.RecordInfo;
import com.hazelcast.map.record.RecordReplicationInfo;
import com.hazelcast.map.record.RecordStatistics;
import com.hazelcast.map.tx.TransactionalMapProxy;
import com.hazelcast.map.wan.MapReplicationRemove;
import com.hazelcast.map.wan.MapReplicationUpdate;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.spi.*;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.transaction.impl.TransactionSupport;
import com.hazelcast.util.*;
import com.hazelcast.util.scheduler.EntryTaskScheduler;
import com.hazelcast.util.scheduler.ScheduledEntry;
import com.hazelcast.wan.WanReplicationEvent;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The SPI Service for the Map.
 *
 * @author enesakar 1/17/13
 */
public class MapService implements ManagedService, MigrationAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, EntryListener>,
        PostJoinAwareService, SplitBrainHandlerService, ReplicationSupportingService {

    public final static String SERVICE_NAME = "hz:impl:mapService";

    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final ConcurrentMap<String, MapContainer> mapContainers = new ConcurrentHashMap<String, MapContainer>();
    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();
    private final AtomicReference<List<Integer>> ownedPartitions;
    private final Map<String, MapMergePolicy> mergePolicyMap;
    // we added following latency to be sure the ongoing migration is completed
    // if the owner of the record could not complete task before migration
    private final long replicaWaitMillisForScheduledTasks;
    private final MapEvictionManager mapEvictionManager;

    public MapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.mapEvictionManager = new MapEvictionManager(this);
        this.replicaWaitMillisForScheduledTasks = TimeUnit.SECONDS.toMillis(getNodeEngine().getGroupProperties()
                .MAP_REPLICA_WAIT_SECONDS_FOR_SCHEDULED_TASKS.getInteger());
        logger = nodeEngine.getLogger(MapService.class.getName());
        partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        ownedPartitions = new AtomicReference<List<Integer>>();
        mergePolicyMap = new ConcurrentHashMap<String, MapMergePolicy>();
        mergePolicyMap.put(PutIfAbsentMapMergePolicy.class.getName(), new PutIfAbsentMapMergePolicy());
        mergePolicyMap.put(HigherHitsMapMergePolicy.class.getName(), new HigherHitsMapMergePolicy());
        mergePolicyMap.put(PassThroughMergePolicy.class.getName(), new PassThroughMergePolicy());
        mergePolicyMap.put(LatestUpdateMapMergePolicy.class.getName(), new LatestUpdateMapMergePolicy());
    }

    private final ConcurrentMap<String, LocalMapStatsImpl> statsMap = new ConcurrentHashMap<String, LocalMapStatsImpl>(1000);
    private final ConstructorFunction<String, LocalMapStatsImpl> localMapStatsConstructorFunction = new ConstructorFunction<String, LocalMapStatsImpl>() {
        public LocalMapStatsImpl createNew(String key) {
            return new LocalMapStatsImpl();
        }
    };

    public LocalMapStatsImpl getLocalMapStatsImpl(String name) {
        return ConcurrencyUtil.getOrPutIfAbsent(statsMap, name, localMapStatsConstructorFunction);
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            partitionContainers[i] = new PartitionContainer(this, i);
        }
        final LockService lockService = nodeEngine.getSharedService(LockService.SERVICE_NAME);
        if (lockService != null) {
            lockService.registerLockStoreConstructor(SERVICE_NAME, new ConstructorFunction<ObjectNamespace, LockStoreInfo>() {
                public LockStoreInfo createNew(final ObjectNamespace key) {
                    final MapContainer mapContainer = getMapContainer(key.getObjectName());
                    return new LockStoreInfo() {
                        public int getBackupCount() {
                            return mapContainer.getBackupCount();
                        }

                        public int getAsyncBackupCount() {
                            return mapContainer.getAsyncBackupCount();
                        }
                    };
                }
            });
        }
        mapEvictionManager.init();
    }

    public void reset() {
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.clear();
            }
        }
        for (NearCache nearCache : nearCacheMap.values()) {
            nearCache.clear();
        }
    }

    public void shutdown(boolean terminate) {
        if (!terminate) {
            flushMapsBeforeShutdown();
            destroyMapStores();
            final PartitionContainer[] containers = partitionContainers;
            for (PartitionContainer container : containers) {
                if (container != null) {
                    container.clear();
                }
            }
            for (NearCache nearCache : nearCacheMap.values()) {
                nearCache.clear();
            }
            nearCacheMap.clear();
            mapContainers.clear();
        }
    }

    private void destroyMapStores() {
        for (MapContainer mapContainer : mapContainers.values()) {
            MapStoreWrapper store = mapContainer.getStore();
            if (store != null) {
                store.destroy();
            }
        }
    }

    private void flushMapsBeforeShutdown() {
        for (PartitionContainer partitionContainer : partitionContainers) {
            for (String mapName : mapContainers.keySet()) {
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                recordStore.setLoaded(true);
                recordStore.flush();
            }
        }
    }

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            return new MapContainer(mapName, nodeEngine.getConfig().findMapConfig(mapName), MapService.this);
        }
    };

    public Operation getPostJoinOperation() {
        PostJoinMapOperation o = new PostJoinMapOperation();
        for (MapContainer mapContainer : mapContainers.values()) {
            o.addMapIndex(mapContainer);
            o.addMapInterceptors(mapContainer);
        }
        return o;
    }

    public Runnable prepareMergeRunnable() {
        Map<MapContainer, Collection<Record>> recordMap = new HashMap<MapContainer, Collection<Record>>(mapContainers.size());
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        int partitionCount = partitionService.getPartitionCount();
        Address thisAddress = nodeEngine.getClusterService().getThisAddress();

        for (MapContainer mapContainer : mapContainers.values()) {
            for (int i = 0; i < partitionCount; i++) {
                RecordStore recordStore = getPartitionContainer(i).getRecordStore(mapContainer.getName());
                // add your owned entries to the map so they will be merged
                if (thisAddress.equals(partitionService.getPartitionOwner(i))) {
                    Collection<Record> records = recordMap.get(mapContainer);
                    if (records == null) {
                        records = new ArrayList<Record>();
                        recordMap.put(mapContainer, records);
                    }
                    records.addAll(recordStore.getReadonlyRecordMap().values());
                }
                // clear all records either owned or backup
                recordStore.reset();
            }
        }
        return new Merger(recordMap);
    }

    @Override
    public void onReplicationEvent(WanReplicationEvent replicationEvent) {
        Object eventObject = replicationEvent.getEventObject();
        if (eventObject instanceof MapReplicationUpdate) {
            MapReplicationUpdate replicationUpdate = (MapReplicationUpdate) eventObject;
            EntryView entryView = replicationUpdate.getEntryView();
            MapMergePolicy mergePolicy = replicationUpdate.getMergePolicy();
            String mapName = replicationUpdate.getMapName();
            MapContainer mapContainer = getMapContainer(mapName);
            MergeOperation operation = new MergeOperation(mapName, toData(entryView.getKey(), mapContainer.getPartitioningStrategy()), entryView, mergePolicy);
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(entryView.getKey());
                Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        } else if (eventObject instanceof MapReplicationRemove) {
            MapReplicationRemove replicationRemove = (MapReplicationRemove) eventObject;
            WanOriginatedDeleteOperation operation = new WanOriginatedDeleteOperation(replicationRemove.getMapName(), replicationRemove.getKey());
            try {
                int partitionId = nodeEngine.getPartitionService().getPartitionId(replicationRemove.getKey());
                Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
                f.get();
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    public MapMergePolicy getMergePolicy(String mergePolicyName) {
        MapMergePolicy mergePolicy = mergePolicyMap.get(mergePolicyName);
        if (mergePolicy == null && mergePolicyName != null) {
            try {

                // check if user has entered custom class name instead of policy name
                mergePolicy = ClassLoaderUtil.newInstance(nodeEngine.getConfigClassLoader(), mergePolicyName);
                mergePolicyMap.put(mergePolicyName, mergePolicy);
            } catch (Exception e) {
                logger.severe(e);
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (mergePolicy == null) {
            return mergePolicyMap.get(MapConfig.DEFAULT_MAP_MERGE_POLICY);
        }
        return mergePolicy;
    }

    public class Merger implements Runnable {

        Map<MapContainer, Collection<Record>> recordMap;

        public Merger(Map<MapContainer, Collection<Record>> recordMap) {
            this.recordMap = recordMap;
        }

        public void run() {
            for (final MapContainer mapContainer : recordMap.keySet()) {
                Collection<Record> recordList = recordMap.get(mapContainer);
                String mergePolicyName = mapContainer.getMapConfig().getMergePolicy();
                MapMergePolicy mergePolicy = getMergePolicy(mergePolicyName);

                // todo number of records may be high. below can be optimized a many records can be send in single invocation
                final MapMergePolicy finalMergePolicy = mergePolicy;
                for (final Record record : recordList) {
                    // todo too many submission. should submit them in subgroups
                    nodeEngine.getExecutionService().submit("hz:map-merge", new Runnable() {
                        public void run() {
                            final SimpleEntryView entryView = createSimpleEntryView(record.getKey(), toData(record.getValue()),record);
                            MergeOperation operation = new MergeOperation(mapContainer.getName(), record.getKey(), entryView, finalMergePolicy);
                            try {
                                int partitionId = nodeEngine.getPartitionService().getPartitionId(record.getKey());
                                Future f = nodeEngine.getOperationService().invokeOnPartition(SERVICE_NAME, operation, partitionId);
                                f.get();
                            } catch (Throwable t) {
                                ExceptionUtil.rethrow(t);
                            }
                        }
                    });
                }
            }
        }

    }

    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutSynchronized(mapContainers, mapName, mapContainers, mapConstructor);
    }

    public Map<String, MapContainer> getMapContainers() {
        return Collections.unmodifiableMap(mapContainers);
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    public RecordStore getExistingRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getExistingRecordStore(mapName);
    }

    public List<Integer> getOwnedPartitions() {
        List<Integer> partitions = ownedPartitions.get();
        if (partitions == null) {
            partitions = getMemberPartitions();
            ownedPartitions.set(partitions);
        }
        return partitions;
    }

    private List<Integer> getMemberPartitions() {
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        List<Integer> partitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        return Collections.unmodifiableList(partitions);
    }

    public void beforeMigration(PartitionMigrationEvent event) {
    }

    public Operation prepareReplicationOperation(PartitionReplicationEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        final MapReplicationOperation operation = new MapReplicationOperation(this, container, event.getPartitionId(), event.getReplicaIndex());
        return operation.isEmpty() ? null : operation;
    }

    public void commitMigration(PartitionMigrationEvent event) {
        migrateIndex(event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            clearPartitionData(event.getPartitionId());
        }
        ownedPartitions.set(getMemberPartitions());
    }

    private void migrateIndex(PartitionMigrationEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        for (RecordStore recordStore : container.getMaps().values()) {
            final MapContainer mapContainer = getMapContainer(recordStore.getName());
            final IndexService indexService = mapContainer.getIndexService();
            if (indexService.hasIndex()) {
                for (Record record : recordStore.getReadonlyRecordMap().values()) {
                    if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                        indexService.removeEntryIndex(record.getKey());
                    } else {
                        Object value = record.getValue();
                        if (value != null) {
                            indexService.saveEntryIndex(new QueryEntry(getSerializationService(), record.getKey(), record.getKey(), value));
                        }
                    }
                }
            }
        }
    }

    public void rollbackMigration(PartitionMigrationEvent event) {
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionData(event.getPartitionId());
        }
        ownedPartitions.set(getMemberPartitions());
    }

    private void clearPartitionData(final int partitionId) {
        final PartitionContainer container = partitionContainers[partitionId];
        if (container != null) {
            for (RecordStore mapPartition : container.getMaps().values()) {
                mapPartition.clearPartition();
            }
            container.getMaps().clear();
        }
    }

    public void clearPartitionReplica(int partitionId) {
        clearPartitionData(partitionId);
    }

    public Record createRecord(String name, Data dataKey, Object value, long ttl) {
        return createRecord(name, dataKey, value, ttl, true);
    }

    public Record createRecord(String name, Data dataKey, Object value, long ttl, boolean shouldSchedule) {
        MapContainer mapContainer = getMapContainer(name);
        Record record = mapContainer.getRecordFactory().newRecord(dataKey, value);

        if (shouldSchedule) {
            // if ttl is 0 then no eviction. if ttl is -1 then default configured eviction is applied
            if (ttl < 0 && mapContainer.getMapConfig().getTimeToLiveSeconds() > 0) {
                final long delay = TimeUnit.SECONDS.toMillis(mapContainer.getMapConfig().getTimeToLiveSeconds());
                scheduleTtlEviction(name, record, delay);
            } else if (ttl > 0) {
                scheduleTtlEviction(name, record, ttl);
            }
            if (mapContainer.getMapConfig().getMaxIdleSeconds() > 0) {
                final long delay = TimeUnit.SECONDS.toMillis(mapContainer.getMapConfig().getMaxIdleSeconds());
                scheduleIdleEviction(name, dataKey, delay);
            }
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    public TransactionalMapProxy createTransactionalObject(String name, TransactionSupport transaction) {
        return new TransactionalMapProxy(name, this, nodeEngine, transaction);
    }

    @Override
    public void rollbackTransaction(String transactionId) {

    }

    private final ConstructorFunction<String, NearCache> nearCacheConstructor = new ConstructorFunction<String, NearCache>() {
        public NearCache createNew(String mapName) {
            return new NearCache(mapName, MapService.this);
        }
    };

    public NearCache getNearCache(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(nearCacheMap, mapName, nearCacheConstructor);
    }

    // this operation returns the given value in near-cache memory format (data or object)
    // if near-cache is not enabled, it returns null
    public Object putNearCache(String mapName, Data key, Data value) {
        if (!isNearCacheEnabled(mapName)) {
            return null;
        }
        NearCache nearCache = getNearCache(mapName);
        return nearCache.put(key, value);
    }

    public void invalidateNearCache(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getNearCache(mapName);
        nearCache.invalidate(key);
    }

    public void invalidateNearCache(String mapName, Set<Data> keys) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        NearCache nearCache = getNearCache(mapName);
        nearCache.invalidate(keys);
    }

    public void clearNearCache(String mapName) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        final NearCache nearCache = nearCacheMap.get(mapName);
        if (nearCache != null) {
            nearCache.clear();
        }
    }

    public void invalidateAllNearCaches(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                if (member.localMember())
                    continue;
                Operation operation = new InvalidateNearCacheOperation(mapName, key).setServiceName(SERVICE_NAME);
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
        // below local invalidation is for the case the data is cached before partition is owned/migrated
        invalidateNearCache(mapName, key);
    }


    public boolean isNearCacheAndInvalidationEnabled(String mapName) {
        final MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.isNearCacheEnabled()
                && mapContainer.getMapConfig().getNearCacheConfig().isInvalidateOnChange();
    }

    public boolean isNearCacheEnabled(String mapName) {
        final MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.isNearCacheEnabled();
    }

    public void invalidateAllNearCaches(String mapName, Set<Data> keys) {
        if (!isNearCacheEnabled(mapName)) {
            return;
        }
        if (keys == null || keys.isEmpty()) {
            return;
        }
        //send operation.
        Operation operation = new NearCacheKeySetInvalidationOperation(mapName, keys).setServiceName(SERVICE_NAME);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                if (member.localMember())
                    continue;
                nodeEngine.getOperationService().send(operation, member.getAddress());
            } catch (Throwable throwable) {
                logger.warning(throwable);
            }
        }
        // below local invalidation is for the case the data is cached before partition is owned/migrated
        for (final Data key : keys) {
            invalidateNearCache(mapName, key);
        }
    }

    public Object getFromNearCache(String mapName, Data key) {
        if (!isNearCacheEnabled(mapName)) {
            return null;
        }
        NearCache nearCache = getNearCache(mapName);
        return nearCache.get(key);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public MapProxyImpl createDistributedObject(String name) {
        return new MapProxyImpl(name, this, nodeEngine);
    }

    public void destroyDistributedObject(String name) {
        MapContainer mapContainer = mapContainers.remove(name);
        if (mapContainer != null) {
            if (mapContainer.isNearCacheEnabled()) {
                NearCache nearCache = nearCacheMap.remove(name);
                if (nearCache != null) {
                    nearCache.clear();
                }
            }
            mapContainer.shutDownMapStoreScheduledExecutor();
        }
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.destroyMap(name);
            }
        }
        nodeEngine.getEventService().deregisterAllListeners(SERVICE_NAME, name);
    }

    public String addInterceptor(String mapName, MapInterceptor interceptor) {
        return getMapContainer(mapName).addInterceptor(interceptor);
    }

    public void removeInterceptor(String mapName, String id) {
        getMapContainer(mapName).removeInterceptor(id);
    }

    // todo interceptors should get a wrapped object which includes the serialized version
    public Object interceptGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptGet(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    public void interceptAfterGet(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterGet(value);
            }
        }
    }

    public Object interceptPut(String mapName, Object oldValue, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(newValue);
            oldValue = toObject(oldValue);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptPut(oldValue, result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? newValue : result;
    }

    public void interceptAfterPut(String mapName, Object newValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            newValue = toObject(newValue);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterPut(newValue);
            }
        }
    }

    public Object interceptRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = null;
        if (!interceptors.isEmpty()) {
            result = toObject(value);
            for (MapInterceptor interceptor : interceptors) {
                Object temp = interceptor.interceptRemove(result);
                if (temp != null) {
                    result = temp;
                }
            }
        }
        return result == null ? value : result;
    }

    public void interceptAfterRemove(String mapName, Object value) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        if (!interceptors.isEmpty()) {
            for (MapInterceptor interceptor : interceptors) {
                value = toObject(value);
                interceptor.afterRemove(value);
            }
        }
    }

    public void publishWanReplicationUpdate(String mapName, EntryView entryView) {
        MapContainer mapContainer = getMapContainer(mapName);
        MapReplicationUpdate replicationEvent = new MapReplicationUpdate(mapName, mapContainer.getWanMergePolicy(), entryView);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(SERVICE_NAME, replicationEvent);
    }

    public void publishWanReplicationRemove(String mapName, Data key, long removeTime) {
        MapContainer mapContainer = getMapContainer(mapName);
        MapReplicationRemove replicationEvent = new MapReplicationRemove(mapName, key, removeTime);
        mapContainer.getWanReplicationPublisher().publishReplicationEvent(SERVICE_NAME, replicationEvent);
    }

    public void publishEvent(Address caller, String mapName, EntryEventType eventType, Data dataKey, Data dataOldValue, Data dataValue) {
        Collection<EventRegistration> candidates = nodeEngine.getEventService().getRegistrations(SERVICE_NAME, mapName);
        Set<EventRegistration> registrationsWithValue = new HashSet<EventRegistration>();
        Set<EventRegistration> registrationsWithoutValue = new HashSet<EventRegistration>();
        if (candidates.isEmpty())
            return;
        Object key = null;
        Object value = null;
        Object oldValue = null;
        for (EventRegistration candidate : candidates) {
            EventFilter filter = candidate.getFilter();
            if (filter instanceof EventServiceImpl.EmptyFilter) {
                registrationsWithValue.add(candidate);
            } else if (filter instanceof QueryEventFilter) {
                Object testValue;
                if (eventType == EntryEventType.REMOVED || eventType == EntryEventType.EVICTED) {
                    oldValue = oldValue != null ? oldValue : toObject(dataOldValue);
                    testValue = oldValue;
                } else {
                    value = value != null ? value : toObject(dataValue);
                    testValue = value;
                }
                key = key != null ? key : toObject(dataKey);
                QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
                QueryEntry entry = new QueryEntry(getSerializationService(), dataKey, key, testValue);
                if (queryEventFilter.eval(entry)) {
                    if (queryEventFilter.isIncludeValue()) {
                        registrationsWithValue.add(candidate);
                    } else {
                        registrationsWithoutValue.add(candidate);
                    }
                }
            } else if (filter.eval(dataKey)) {
                EntryEventFilter eventFilter = (EntryEventFilter) filter;
                if (eventFilter.isIncludeValue()) {
                    registrationsWithValue.add(candidate);
                } else {
                    registrationsWithoutValue.add(candidate);
                }
            }
        }
        if (registrationsWithValue.isEmpty() && registrationsWithoutValue.isEmpty())
            return;
        String source = nodeEngine.getThisAddress().toString();
        if (eventType == EntryEventType.REMOVED || eventType == EntryEventType.EVICTED) {
            dataValue = dataValue != null ? dataValue : dataOldValue;
        }
        EventData event = new EventData(source, mapName, caller, dataKey, dataValue, dataOldValue, eventType.getType());
        int orderKey = dataKey.hashCode();
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithValue, event, orderKey);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithoutValue, event.cloneWithoutValues(), orderKey);
    }

    public String addLocalEventListener(EntryListener entryListener, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, mapName, entryListener);
        return registration.getId();
    }

    public String addLocalEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, mapName, eventFilter, entryListener);
        return registration.getId();
    }

    public String addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, mapName, eventFilter, entryListener);
        return registration.getId();
    }

    public boolean removeEventListener(String mapName, String registrationId) {
        return nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    public void applyRecordInfo(Record record, String mapName, RecordInfo replicationInfo) {
        record.setStatistics(replicationInfo.getStatistics());
        record.setVersion(replicationInfo.getVersion());
        record.setEvictionCriteriaNumber(replicationInfo.getEvictionCriteriaNumber());

        if (replicationInfo.getIdleDelayMillis() >= 0) {
            scheduleIdleEviction(mapName, record.getKey(), replicationInfo.getIdleDelayMillis());
        }
        if (replicationInfo.getTtlDelayMillis() >= 0) {
            scheduleTtlEviction(mapName, record, replicationInfo.getTtlDelayMillis());
        }
        if (replicationInfo.getMapStoreWriteDelayMillis() >= 0) {
            scheduleMapStoreWrite(mapName, record.getKey(), record.getValue(), replicationInfo.getMapStoreWriteDelayMillis());
        }
        if (replicationInfo.getMapStoreDeleteDelayMillis() >= 0) {
            scheduleMapStoreDelete(mapName, record.getKey(), replicationInfo.getMapStoreDeleteDelayMillis());
        }
    }

    public RecordReplicationInfo createRecordReplicationInfo(MapContainer mapContainer, Record record) {
        final RecordInfo info = constructRecordInfo(mapContainer, record, replicaWaitMillisForScheduledTasks);
        final RecordReplicationInfo replicationInfo
                = new RecordReplicationInfo(record.getKey(), toData(record.getValue()), info);
        return replicationInfo;
    }

    public RecordInfo createRecordInfo(MapContainer mapContainer, Record record) {
        // this info is created to be used in backups.
        // we added following latency (10 seconds) to be sure the ongoing promotion is
        // completed if the owner of the record could not complete task before promotion
        return constructRecordInfo(mapContainer, record, replicaWaitMillisForScheduledTasks);
    }

    private RecordInfo constructRecordInfo(MapContainer mapContainer, Record record, long extraDelay) {
        final RecordInfo info = new RecordInfo();
        info.setStatistics(record.getStatistics());
        info.setVersion(record.getVersion());
        info.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());
        setDelays(mapContainer, info, record.getKey(), extraDelay);
        return info;
    }

    private void setDelays(MapContainer mapContainer, RecordInfo info, Data key, long extraDelay) {
        long deleteDelay = -1;
        long writeDelay = -1;
        long idleDelay;
        long ttlDelay;
        if (mapContainer.getMapStoreScheduler() != null) {
            final ScheduledEntry scheduledEntry = mapContainer.getMapStoreScheduler().get(key);
            if (scheduledEntry != null) {
                if (scheduledEntry.getValue() == null) {
                    deleteDelay = extraDelay + findDelayMillis(scheduledEntry);
                } else {
                    writeDelay = extraDelay + findDelayMillis(scheduledEntry);
                }
            }
        }
        idleDelay = getDelay(mapContainer.getIdleEvictionScheduler(), key, extraDelay);
        ttlDelay = getDelay(mapContainer.getTtlEvictionScheduler(), key, extraDelay);
        // set delays.
        info.setMapStoreDeleteDelayMillis(deleteDelay);
        info.setMapStoreWriteDelayMillis(writeDelay);
        info.setIdleDelayMillis(idleDelay);
        info.setTtlDelayMillis(ttlDelay);
    }

    private long getDelay(EntryTaskScheduler entryTaskScheduler, Data key, long extraDelay) {
        if (entryTaskScheduler != null) {
            final ScheduledEntry entry = entryTaskScheduler.get(key);
            if (entry != null) {
                return extraDelay + findDelayMillis(entry);
            }
        }
        return -1;
    }

    public <K,V> SimpleEntryView<K,V> createSimpleEntryView(K key, V value, Record record) {
        final SimpleEntryView simpleEntryView = new SimpleEntryView(key, value);
        simpleEntryView.setCost(record.getCost());
        simpleEntryView.setVersion(record.getVersion());
        simpleEntryView.setEvictionCriteriaNumber(record.getEvictionCriteriaNumber());

        final RecordStatistics statistics = record.getStatistics();
        if (statistics != null) {
            simpleEntryView.setHits(statistics.getHits());
            simpleEntryView.setCreationTime(statistics.getCreationTime());
            simpleEntryView.setExpirationTime(statistics.getExpirationTime());
            simpleEntryView.setLastAccessTime(statistics.getLastAccessTime());
            simpleEntryView.setLastStoredTime(statistics.getLastStoredTime());
            simpleEntryView.setLastUpdateTime(statistics.getLastUpdateTime());
        }
       return simpleEntryView;
    }

    public long findDelayMillis(ScheduledEntry entry) {
        final long timeElapsedUntilNow = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - entry.getScheduleStartTimeInNanos());
        final long remainingTime = entry.getScheduledDelayMillis() - timeElapsedUntilNow;
        return Math.max(0, remainingTime);
    }

    public Object toObject(Object data) {
        if (data == null)
            return null;
        if (data instanceof Data) {
            return nodeEngine.toObject(data);
        } else {
            return data;
        }
    }

    public Data toData(Object object, PartitioningStrategy partitionStrategy) {
        if (object == null)
            return null;
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object, partitionStrategy);
        }
    }

    public Data toData(Object object) {
        if (object == null)
            return null;
        if (object instanceof Data) {
            return (Data) object;
        } else {
            return nodeEngine.getSerializationService().toData(object);
        }
    }

    public boolean compare(String mapName, Object value1, Object value2) {
        if (value1 == null && value2 == null) {
            return true;
        }
        if (value1 == null) {
            return false;
        }
        if (value2 == null) {
            return false;
        }

        MapContainer mapContainer = getMapContainer(mapName);
        return mapContainer.getRecordFactory().isEquals(value1, value2);
    }

    @SuppressWarnings("unchecked")
    public void dispatchEvent(EventData eventData, EntryListener listener) {
        Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
        EntryEvent event = new DataAwareEntryEvent(member, eventData.getEventType(), eventData.getMapName(),
                eventData.getDataKey(), eventData.getDataNewValue(), eventData.getDataOldValue(), getSerializationService());
        switch (event.getEventType()) {
            case ADDED:
                listener.entryAdded(event);
                break;
            case EVICTED:
                listener.entryEvicted(event);
                break;
            case UPDATED:
                listener.entryUpdated(event);
                break;
            case REMOVED:
                listener.entryRemoved(event);
                break;
            default:
                throw new IllegalArgumentException("Invalid event type: " + event.getEventType());
        }
        MapContainer mapContainer = getMapContainer(eventData.getMapName());
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            getLocalMapStatsImpl(eventData.getMapName()).incrementReceivedEvents();
        }
    }

    public void scheduleIdleEviction(String mapName, Data key, long delay) {
        getMapContainer(mapName).getIdleEvictionScheduler().schedule(delay, key, null);
    }

    public void scheduleTtlEviction(String mapName, Record record, long delay) {
        if (record.getStatistics() != null) {
            record.getStatistics().setExpirationTime(Clock.currentTimeMillis() + delay);
        }
        getMapContainer(mapName).getTtlEvictionScheduler().schedule(delay, toData(record.getKey()), null);
    }

    public void scheduleMapStoreWrite(String mapName, Data key, Object value, long delay) {
        getMapContainer(mapName).getMapStoreScheduler().schedule(delay, key, value);
    }

    public void scheduleMapStoreDelete(String mapName, Data key, long delay) {
        getMapContainer(mapName).getMapStoreScheduler().schedule(delay, key, null);
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    public QueryResult queryOnPartition(String mapName, Predicate predicate, int partitionId) {
        final QueryResult result = new QueryResult();
        List<QueryEntry> list = new LinkedList<QueryEntry>();
        PartitionContainer container = getPartitionContainer(partitionId);
        RecordStore recordStore = container.getRecordStore(mapName);
        Map<Data, Record> records = recordStore.getReadonlyRecordMap();
        SerializationService serializationService = nodeEngine.getSerializationService();
        final PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        for (Record record : records.values()) {
            Data key = record.getKey();
            Object value = record.getValue();
            if (value == null) {
                continue;
            }
            QueryEntry queryEntry = new QueryEntry(serializationService, key, key, value);
            if (predicate.apply(queryEntry)) {
                if (pagingPredicate != null) {
                    Map.Entry anchor = pagingPredicate.getAnchor();
                    if (anchor != null &&
                            SortingUtil.compare(pagingPredicate.getComparator(), pagingPredicate.getIterationType(), anchor, queryEntry) >= 0) {
                        continue;
                    }
                }
                list.add(queryEntry);
            }
        }
        if (pagingPredicate != null) {
            Collections.sort(list, wrapperComparator);
            if (list.size() > pagingPredicate.getPageSize()) {
                list = list.subList(0, pagingPredicate.getPageSize());
            }
        }
        for (QueryEntry entry : list) {
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
        }
        return result;
    }

    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        MapContainer mapContainer = getMapContainer(mapName);
        LocalMapStatsImpl localMapStats = getLocalMapStatsImpl(mapName);
        if (!mapContainer.getMapConfig().isStatisticsEnabled()) {
            return localMapStats;
        }

        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long dirtyCount = 0;
        long ownedEntryMemoryCost = 0;
        long backupEntryMemoryCost = 0;
        long hits = 0;
        long lockedEntryCount = 0;
        long heapCost = 0;

        int backupCount = mapContainer.getTotalBackupCount();
        ClusterService clusterService = nodeEngine.getClusterService();
        final InternalPartitionService partitionService = nodeEngine.getPartitionService();

        Address thisAddress = clusterService.getThisAddress();
        for (int partitionId = 0; partitionId < partitionService.getPartitionCount(); partitionId++) {
            InternalPartition partition = partitionService.getPartition(partitionId);
            Address owner = partition.getOwner();
            if (owner == null) {
                //no-op because no owner is set yet. Therefor we don't know anything about the map
            } else if (owner.equals(thisAddress)) {
                PartitionContainer partitionContainer = getPartitionContainer(partitionId);
                RecordStore recordStore = partitionContainer.getExistingRecordStore(mapName);

                //we don't want to force loading the record store because we are loading statistics. So that is why
                //we ask for 'getExistingRecordStore' instead of 'getRecordStore' which does the load.
                if (recordStore != null) {
                    heapCost += recordStore.getHeapCost();
                    Map<Data, Record> records = recordStore.getReadonlyRecordMap();
                    for (Record record : records.values()) {
                        RecordStatistics stats = record.getStatistics();
                        // there is map store and the record is dirty (waits to be stored)
                        ownedEntryCount++;
                        ownedEntryMemoryCost += record.getCost();
                        localMapStats.setLastAccessTime(stats.getLastAccessTime());
                        localMapStats.setLastUpdateTime(stats.getLastUpdateTime());
                        hits += stats.getHits();
                        if (recordStore.isLocked(record.getKey())) {
                            lockedEntryCount++;
                        }
                    }
                }
            } else {
                for (int replica = 1; replica <= backupCount; replica++) {
                    Address replicaAddress = partition.getReplicaAddress(replica);
                    int tryCount = 30;
                    // wait if the partition table is not updated yet
                    while (replicaAddress == null && clusterService.getSize() > backupCount && tryCount-- > 0) {
                        try {
                            TimeUnit.MILLISECONDS.sleep(100);
                        } catch (InterruptedException e) {
                            throw ExceptionUtil.rethrow(e);
                        }
                        replicaAddress = partition.getReplicaAddress(replica);
                    }

                    if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                        PartitionContainer partitionContainer = getPartitionContainer(partitionId);
                        RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                        heapCost += recordStore.getHeapCost();

                        Map<Data, Record> records = recordStore.getReadonlyRecordMap();
                        for (Record record : records.values()) {
                            backupEntryCount++;
                            backupEntryMemoryCost += record.getCost();
                        }
                    } else if (replicaAddress == null && clusterService.getSize() > backupCount) {
                        logger.warning("Partition: " + partition + ", replica: " + replica + " has no owner!");
                    }
                }
            }
        }

        if (mapContainer.getMapStoreScheduler() != null) {
            dirtyCount = mapContainer.getMapStoreScheduler().size();
        }
        localMapStats.setBackupCount(backupCount);
        localMapStats.setDirtyEntryCount(zeroOrPositive(dirtyCount));
        localMapStats.setLockedEntryCount(zeroOrPositive(lockedEntryCount));
        localMapStats.setHits(zeroOrPositive(hits));
        localMapStats.setOwnedEntryCount(zeroOrPositive(ownedEntryCount));
        localMapStats.setBackupEntryCount(zeroOrPositive(backupEntryCount));
        localMapStats.setOwnedEntryMemoryCost(zeroOrPositive(ownedEntryMemoryCost));
        localMapStats.setBackupEntryMemoryCost(zeroOrPositive(backupEntryMemoryCost));
        // add near cache heap cost.
        heapCost += mapContainer.getNearCacheSizeEstimator().getSize();
        localMapStats.setHeapCost(heapCost);
        if (mapContainer.getMapConfig().isNearCacheEnabled()) {
            NearCacheStatsImpl nearCacheStats = getNearCache(mapName).getNearCacheStats();
            localMapStats.setNearCacheStats(nearCacheStats);
        }

        return localMapStats;
    }

    static long zeroOrPositive(long value) {
        return (value > 0) ? value : 0;
    }

}
