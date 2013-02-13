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

import com.hazelcast.client.ClientCommandHandler;
import com.hazelcast.cluster.ClusterServiceImpl;
import com.hazelcast.cluster.JoinOperation;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.concurrent.lock.LockInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.client.*;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceImpl;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.TransactionException;
import com.hazelcast.spi.impl.EventServiceImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConcurrencyUtil.ConstructorFunction;
import com.hazelcast.util.Util;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

public class MapService implements ManagedService, MigrationAwareService, MembershipAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, EntryListener>,
        ClientProtocolService, PostJoinAwareService, SplitBrainHandlerService {

    public final static String SERVICE_NAME = "hz:impl:mapService";

    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final PartitionContainer[] partitionContainers;
    private final AtomicLong counter = new AtomicLong(new Random().nextLong());
    private final ConcurrentMap<String, MapContainer> mapContainers = new ConcurrentHashMap<String, MapContainer>();
    private final ConcurrentMap<String, NearCache> nearCacheMap = new ConcurrentHashMap<String, NearCache>();
    private final ConcurrentMap<ListenerKey, String> eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();
    private final AtomicReference<List<Integer>> ownedPartitions;

    public MapService(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(MapService.class.getName());
        partitionContainers = new PartitionContainer[nodeEngine.getPartitionService().getPartitionCount()];
        ownedPartitions = new AtomicReference<List<Integer>>();
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            PartitionInfo partition = nodeEngine.getPartitionService().getPartitionInfo(i);
            partitionContainers[i] = new PartitionContainer(this, partition);
        }
        nodeEngine.getExecutionService().scheduleAtFixedRate(new MapEvictTask(), 3, 1, TimeUnit.SECONDS);
    }

    private final ConstructorFunction<String, MapContainer> mapConstructor = new ConstructorFunction<String, MapContainer>() {
        public MapContainer createNew(String mapName) {
            return new MapContainer(mapName, nodeEngine.getConfig().getMapConfig(mapName));
        }
    };

    public Operation getPostJoinOperation() {
        PostJoinMapOperation o = new PostJoinMapOperation();
        for (MapContainer mapContainer : mapContainers.values()) {
            o.addMapIndex(mapContainer);
        }
        return o;
    }

    public Runnable prepareMergeRunnable() {
        // TODO: @mm - create merge runnable according to map merge policies.
        return null;
    }

    public static class PostJoinMapOperation extends AbstractOperation implements JoinOperation {
        private List<MapIndexInfo> lsMapIndexes = new LinkedList<MapIndexInfo>();

        @Override
        public String getServiceName() {
            return SERVICE_NAME;
        }

        void addMapIndex(MapContainer mapContainer) {
            final IndexService indexService = mapContainer.getIndexService();
            if (indexService.hasIndex()) {
                MapIndexInfo mapIndexInfo = new MapIndexInfo(mapContainer.getName());
                for (Index index : indexService.getIndexes()) {
                    mapIndexInfo.addIndexInfo(index.getAttributeName(), index.isOrdered());
                }
                lsMapIndexes.add(mapIndexInfo);
            }
        }

        class MapIndexInfo implements DataSerializable {
            String mapName;
            private List<IndexInfo> lsIndexes = new LinkedList<IndexInfo>();

            public MapIndexInfo(String mapName) {
                this.mapName = mapName;
            }

            public MapIndexInfo() {
            }

            class IndexInfo implements DataSerializable {
                String attributeName;
                boolean ordered;

                IndexInfo() {
                }

                IndexInfo(String attributeName, boolean ordered) {
                    this.attributeName = attributeName;
                    this.ordered = ordered;
                }

                public void writeData(ObjectDataOutput out) throws IOException {
                    out.writeUTF(attributeName);
                    out.writeBoolean(ordered);
                }

                public void readData(ObjectDataInput in) throws IOException {
                    attributeName = in.readUTF();
                    ordered = in.readBoolean();
                }
            }

            public void addIndexInfo(String attributeName, boolean ordered) {
                lsIndexes.add(new IndexInfo(attributeName, ordered));
            }

            public void writeData(ObjectDataOutput out) throws IOException {
                out.writeUTF(mapName);
                out.writeInt(lsIndexes.size());
                for (IndexInfo indexInfo : lsIndexes) {
                    indexInfo.writeData(out);
                }
            }

            public void readData(ObjectDataInput in) throws IOException {
                mapName = in.readUTF();
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    IndexInfo indexInfo = new IndexInfo();
                    indexInfo.readData(in);
                    lsIndexes.add(indexInfo);
                }
            }
        }

        @Override
        public void run() throws Exception {
            MapService mapService = getService();
            for (MapIndexInfo mapIndex : lsMapIndexes) {
                final MapContainer mapContainer = mapService.getMapContainer(mapIndex.mapName);
                final IndexService indexService = mapContainer.getIndexService();
                for (MapIndexInfo.IndexInfo indexInfo : mapIndex.lsIndexes) {
                    indexService.addOrGetIndex(indexInfo.attributeName, indexInfo.ordered);
                }
            }
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeInt(lsMapIndexes.size());
            for (MapIndexInfo mapIndex : lsMapIndexes) {
                mapIndex.writeData(out);
            }
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                MapIndexInfo mapIndexInfo = new MapIndexInfo();
                mapIndexInfo.readData(in);
                lsMapIndexes.add(mapIndexInfo);
            }
        }
    }

    public MapContainer getMapContainer(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(mapContainers, mapName, mapConstructor);
    }

    public PartitionContainer getPartitionContainer(int partitionId) {
        return partitionContainers[partitionId];
    }

    public RecordStore getRecordStore(int partitionId, String mapName) {
        return getPartitionContainer(partitionId).getRecordStore(mapName);
    }

    public long nextId() {
        return counter.incrementAndGet();
    }

    public AtomicReference<List<Integer>> getOwnedPartitions() {
        if (ownedPartitions.get() == null) {
            ownedPartitions.set(nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress()));
        }
        return ownedPartitions;
    }

    public void beforeMigration(MigrationServiceEvent event) {
        // TODO: @mm - what if partition has transactions?
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        final MapMigrationOperation operation = new MapMigrationOperation(container, event.getPartitionId(), event.getReplicaIndex());
        return operation.isEmpty() ? null : operation;
    }

    public void commitMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "Committing " + event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            migrateIndex(event);
            if (event.getMigrationType() == MigrationType.MOVE) {
                clearPartitionData(event.getPartitionId());
            } else if (event.getMigrationType() == MigrationType.MOVE_COPY_BACK) {
                final PartitionContainer container = partitionContainers[event.getPartitionId()];
                for (PartitionRecordStore mapPartition : container.maps.values()) {
                    final MapContainer mapContainer = getMapContainer(mapPartition.name);
                    final MapConfig mapConfig = mapContainer.getMapConfig();
                    if (mapConfig.getTotalBackupCount() < event.getCopyBackReplicaIndex()) {
                        mapPartition.clear();
                    }
                }
            }
        } else {
            migrateIndex(event);
        }
        ownedPartitions.set(nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress()));
    }

    private void migrateIndex(MigrationServiceEvent event) {
        if (event.getReplicaIndex() == 0) {
            final PartitionContainer container = partitionContainers[event.getPartitionId()];
            for (PartitionRecordStore mapPartition : container.maps.values()) {
                final MapContainer mapContainer = getMapContainer(mapPartition.name);
                final IndexService indexService = mapContainer.getIndexService();
                if (indexService.hasIndex()) {
                    for (Record record : mapPartition.getRecords().values()) {
                        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
                            indexService.removeEntryIndex(record.getKey());
                        } else {
                            indexService.saveEntryIndex(new QueryEntry(getSerializationService(), record.getKey(), record.getKey(), record.getValue()));
                        }
                    }
                }
            }
        }
    }

    public void rollbackMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "Rolling back " + event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.DESTINATION) {
            clearPartitionData(event.getPartitionId());
        }
        ownedPartitions.set(nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress()));
    }

    public int getMaxBackupCount() {
        int max = 1;
        for (PartitionContainer container : partitionContainers) {
            max = Math.max(max, container.getMaxBackupCount());
        }
        return max;
    }

    private void clearPartitionData(final int partitionId) {
        logger.log(Level.FINEST, "Clearing partition data -> " + partitionId);
        final PartitionContainer container = partitionContainers[partitionId];
        for (PartitionRecordStore mapPartition : container.maps.values()) {
            mapPartition.clear();
        }
        container.maps.clear();
        container.transactions.clear(); // TODO: not sure?
    }

    public Record createRecord(String name, Data dataKey, Object value, long ttl) {
        Record record = null;
        MapContainer mapContainer = getMapContainer(name);
        final MapConfig.RecordType recordType = mapContainer.getMapConfig().getRecordType();
        if (recordType == MapConfig.RecordType.DATA) {
            record = new DataRecord(dataKey, toData(value));
        } else if (recordType == MapConfig.RecordType.OBJECT) {
            record = new ObjectRecord(dataKey, toObject(value));
        } else if (recordType == MapConfig.RecordType.CACHED) {
            record = new CachedDataRecord(dataKey, toData(value));
        } else {
            throw new IllegalArgumentException("Should not happen!");
        }
        if (ttl <= 0 && mapContainer.getMapConfig().getTimeToLiveSeconds() > 0) {
            record.getState().updateTtlExpireTime(mapContainer.getMapConfig().getTimeToLiveSeconds() * 1000);
            scheduleOperation(name, dataKey, mapContainer.getMapConfig().getTimeToLiveSeconds() * 1000);
        }
        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            scheduleOperation(name, record.getKey(), ttl);
        }
        if (mapContainer.getMapConfig().getMaxIdleSeconds() > 0) {
            record.getState().updateIdleExpireTime(mapContainer.getMapConfig().getMaxIdleSeconds() * 1000);
            scheduleOperation(name, dataKey, mapContainer.getMapConfig().getMaxIdleSeconds() * 1000);
        }
        return record;
    }

    public void prepare(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeEngine.getThisAddress() + " MapService prepare " + txnId);
        PartitionContainer pc = partitionContainers[partitionId];
        TransactionLog txnLog = pc.getTransactionLog(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeEngine.getOperationService().takeBackups(SERVICE_NAME, new MapTxnBackupPrepareOperation(txnLog), 0, partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public void commit(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeEngine.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).commit(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeEngine.getOperationService().takeBackups(SERVICE_NAME, new MapTxnBackupCommitOperation(txnId), 0, partitionId,
                    maxBackupCount, 60);
        } catch (Exception ignored) {
            //commit can never fail
        }
    }

    public void rollback(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeEngine.getThisAddress() + " MapService commit " + txnId);
        getPartitionContainer(partitionId).rollback(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeEngine.getOperationService().takeBackups(SERVICE_NAME, new MapTxnBackupRollbackOperation(txnId), 0, partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }


    private final ConstructorFunction<String, NearCache> nearCacheConstructor = new ConstructorFunction<String, NearCache>() {
        public NearCache createNew(String mapName) {
            return new NearCache(mapName, nodeEngine);
        }
    };

    private NearCache getNearCache(String mapName) {
        return ConcurrencyUtil.getOrPutIfAbsent(nearCacheMap, mapName, nearCacheConstructor);
    }

    public void putNearCache(String mapName, Data key, Data value) {
        NearCache nearCache = getNearCache(mapName);
        nearCache.put(key, value);
    }

    public void invalidateNearCache(String mapName, Data key) {
        NearCache nearCache = getNearCache(mapName);
        nearCache.invalidate(key);
    }

    public void invalidateAllNearCaches(String mapName, Data key) {
        InvalidateNearCacheOperation operation = new InvalidateNearCacheOperation(mapName, key);
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        for (MemberImpl member : members) {
            try {
                if (member.localMember())
                    continue;
                Invocation invocation = nodeEngine.getOperationService().createInvocationBuilder(SERVICE_NAME, operation, member.getAddress()).build();
                invocation.invoke();
            } catch (Throwable throwable) {
                throw new HazelcastException(throwable);
            }
        }
        // below local invalidation is for the case the data is cached before partition is owned/migrated
        invalidateNearCache(mapName, key);
    }

    public Data getFromNearCache(String mapName, Data key) {
        NearCache nearCache = getNearCache(mapName);
        return nearCache.get(key);
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public String getServiceName() {
        return SERVICE_NAME;
    }

    public ObjectMapProxy createDistributedObject(Object objectId) {
        return new ObjectMapProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public DataMapProxy createDistributedObjectForClient(Object objectId) {
        return new DataMapProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public void destroyDistributedObject(Object objectId) {
        logger.log(Level.WARNING, "Destroying object: " + objectId);
        final String name = String.valueOf(objectId);
        mapContainers.remove(name);
        final PartitionContainer[] containers = partitionContainers;
        for (PartitionContainer container : containers) {
            if (container != null) {
                container.destroyMap(name);
            }
        }
        nodeEngine.getEventService().deregisterListeners(SERVICE_NAME, name);
    }

    public void memberAdded(final MembershipServiceEvent membershipEvent) {
    }

    public void memberRemoved(final MembershipServiceEvent membershipEvent) {
        MemberImpl member = membershipEvent.getMember();
        releaseMemberLocks(member);
        // TODO: @mm - when a member dies;
        // * release locks
        // * rollback transaction
        // * do not know ?
    }

    private void releaseMemberLocks(MemberImpl member) {
        for (PartitionContainer container : partitionContainers) {
            for (PartitionRecordStore recordStore : container.maps.values()) {
                Map<Data, LockInfo> locks = recordStore.getLocks();
                for (Map.Entry<Data, LockInfo> entry : locks.entrySet()) {
                    if (entry.getValue().getLockOwner().equals(member.getAddress())) {
                        ForceUnlockOperation forceUnlockOperation = new ForceUnlockOperation(recordStore.name, entry.getKey());
                        forceUnlockOperation.setNodeEngine(nodeEngine);
                        forceUnlockOperation.setServiceName(SERVICE_NAME);
                        forceUnlockOperation.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                        forceUnlockOperation.setPartitionId(container.partitionInfo.getPartitionId());
                        nodeEngine.getOperationService().runOperation(forceUnlockOperation);
                        recordStore.forceUnlock(entry.getKey());
                    }
                }
            }
        }
    }


    public void shutdown() {
        final PartitionContainer[] containers = partitionContainers;
        for (int i = 0; i < containers.length; i++) {
            PartitionContainer container = containers[i];
            if (container != null) {
                container.destroy();
            }
            containers[i] = null;
        }
        for (NearCache nearCache : nearCacheMap.values()) {
            nearCache.destroy();
        }
        nearCacheMap.clear();
        mapContainers.clear();
        eventRegistrations.clear();
    }

    public Map<Command, ClientCommandHandler> getCommandsAsMap() {
        Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();
        commandHandlers.put(Command.MGET, new MapGetHandler(this));
        commandHandlers.put(Command.MSIZE, new MapSizeHandler(this));
        commandHandlers.put(Command.MGETALL, new MapGetAllHandler(this));
        commandHandlers.put(Command.MPUT, new MapPutHandler(this));
        commandHandlers.put(Command.MTRYPUT, new MapTryPutHandler(this));
        commandHandlers.put(Command.MSET, new MapSetHandler(this));
        commandHandlers.put(Command.MPUTTRANSIENT, new MapPutTransientHandler(this));
        commandHandlers.put(Command.MLOCK, new MapLockHandler(this));
        commandHandlers.put(Command.MTRYLOCK, new MapLockHandler(this));
        commandHandlers.put(Command.MTRYREMOVE, new MapTryRemoveHandler(this));
        commandHandlers.put(Command.MISLOCKED, new MapIsLockedHandler(this));
        commandHandlers.put(Command.MUNLOCK, new MapUnlockHandler(this));
        commandHandlers.put(Command.MPUTALL, new MapPutAllHandler(this));
        commandHandlers.put(Command.MREMOVE, new MapRemoveHandler(this));
        commandHandlers.put(Command.MCONTAINSKEY, new MapContainsKeyHandler(this));
        commandHandlers.put(Command.MCONTAINSVALUE, new MapContainsValueHandler(this));
        commandHandlers.put(Command.MPUTIFABSENT, new MapPutIfAbsentHandler(this));
        commandHandlers.put(Command.MREMOVEIFSAME, new MapRemoveIfSameHandler(this));
        commandHandlers.put(Command.MREPLACEIFNOTNULL, new MapReplaceIfNotNullHandler(this));
        commandHandlers.put(Command.MREPLACEIFSAME, new MapReplaceIfSameHandler(this));
        commandHandlers.put(Command.MFLUSH, new MapFlushHandler(this));
        commandHandlers.put(Command.MEVICT, new MapEvictHandler(this));
        commandHandlers.put(Command.MENTRYSET, new MapEntrySetHandler(this));
        commandHandlers.put(Command.KEYSET, new KeySetHandler(this));
        commandHandlers.put(Command.MGETENTRY, new MapGetEntryHandler(this));
        commandHandlers.put(Command.MFORCEUNLOCK, new MapForceUnlockHandler(this));
        commandHandlers.put(Command.MLISTEN, new MapListenHandler(this));
        return commandHandlers;
    }

    @Override
    public void onClientDisconnect(String clientUuid) {
        // TODO: @mm - release locks owned by this client.
    }

    public String addInterceptor(String mapName, MapInterceptor interceptor) {
        return getMapContainer(mapName).addInterceptor(interceptor);
    }

    public String removeInterceptor(String mapName, MapInterceptor interceptor) {
        return getMapContainer(mapName).removeInterceptor(interceptor);
    }

    // todo replace oldValue with existingEntry
    public Object intercept(String mapName, MapOperationType operationType, Data key, Object value, Object oldValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        Object result = value;
        // todo needs optimization about serialization (EntryView type should be used as input)
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            Map.Entry existingEntry = new AbstractMap.SimpleEntry(key, toObject(oldValue));
            MapInterceptorContextImpl context = new MapInterceptorContextImpl(mapName, operationType, key, value, existingEntry);
            for (MapInterceptor interceptor : interceptors) {
                result = interceptor.process(context);
                context.setNewValue(toObject(result));
            }
        }
        return result;
    }

    // todo replace oldValue with existingEntry
    public void interceptAfterProcess(String mapName, MapOperationType operationType, Data key, Object value, Object oldValue) {
        List<MapInterceptor> interceptors = getMapContainer(mapName).getInterceptors();
        // todo needs optimization about serialization (EntryView type should be used as input)
        if (!interceptors.isEmpty()) {
            value = toObject(value);
            Map.Entry existingEntry = new AbstractMap.SimpleEntry(key, toObject(oldValue));
            MapInterceptorContext context = new MapInterceptorContextImpl(mapName, operationType, key, value, existingEntry);
            for (MapInterceptor interceptor : interceptors) {
                interceptor.afterProcess(context);
            }
        }
    }

    public void publishEvent(Address caller, String mapName, int eventType, Data dataKey, Data dataOldValue, Data dataValue) {
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
                if (eventType == EntryEvent.TYPE_REMOVED) {
                    oldValue = oldValue != null ? oldValue : toObject(dataOldValue);
                    testValue = oldValue;
                } else {
                    value = value != null ? value : toObject(value);
                    testValue = value;
                }
                key = key != null ? key : toObject(key);
                QueryEventFilter queryEventFilter = (QueryEventFilter) filter;
                if (queryEventFilter.eval(new SimpleMapEntry(key, testValue))) {
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
        EventData event = new EventData(source, mapName, caller, dataKey, dataValue, dataOldValue, eventType);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithValue, event);
        nodeEngine.getEventService().publishEvent(SERVICE_NAME, registrationsWithoutValue, event.cloneWithoutValues());
    }

    public void addLocalEventListener(EntryListener entryListener, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerLocalListener(SERVICE_NAME, mapName, entryListener);
        eventRegistrations.put(new ListenerKey(entryListener, null), registration.getId());
    }

    public void addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerListener(SERVICE_NAME, mapName, eventFilter, entryListener);
        eventRegistrations.put(new ListenerKey(entryListener, ((EntryEventFilter) eventFilter).getKey()), registration.getId());
    }

    public void removeEventListener(EntryListener entryListener, String mapName, Object key) {
        String registrationId = eventRegistrations.get(new ListenerKey(entryListener, key));
        nodeEngine.getEventService().deregisterListener(SERVICE_NAME, mapName, registrationId);
    }

    public Object toObject(Object data) {
        if (data == null)
            return null;
        if (data instanceof Data)
            return nodeEngine.toObject(data);
        else
            return data;
    }

    public Data toData(Object object) {
        if (object == null)
            return null;
        if (object instanceof Data)
            return (Data) object;
        else
            return nodeEngine.toData(object);
    }

    @SuppressWarnings("unchecked")
    public void dispatchEvent(EventData eventData, EntryListener listener) {
        Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
        EntryEvent event = new DataAwareEntryEvent(member, eventData.getEventType(), eventData.getSource(),
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
        }
        getMapContainer(eventData.getMapName()).getMapOperationCounter().incrementReceivedEvents();
    }

    public void scheduleOperation(String mapName, Data key, long executeTime) {
        MapRecordStateOperation stateOperation = new MapRecordStateOperation(mapName, key);
        final MapRecordTask recordTask = new MapRecordTask(nodeEngine, stateOperation,
                nodeEngine.getPartitionService().getPartitionId(key));
        nodeEngine.getExecutionService().schedule(recordTask, executeTime, TimeUnit.MILLISECONDS);
    }

    public SerializationService getSerializationService() {
        return nodeEngine.getSerializationService();
    }

    // todo map evict task is called every second. if load is very high, is it problem? if it is, you can count map-wide puts and fire map-evict in every thousand put
    // todo another "maybe" optimization run clear operation for all maps not just one map
    // todo what if eviction do not complete in 1 second
    private class MapEvictTask implements Runnable {
        public void run() {
            for (MapContainer mapContainer : mapContainers.values()) {
                MapConfig.EvictionPolicy evictionPolicy = mapContainer.getMapConfig().getEvictionPolicy();
                MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
                if (evictionPolicy != MapConfig.EvictionPolicy.NONE && maxSizeConfig.getSize() > 0) {
                    boolean check = checkLimits(mapContainer);
                    if (check) {
                        evictMap(mapContainer);
                    }
                }
            }
        }

        // todo call evict map listeners
        private void evictMap(MapContainer mapContainer) {
            MapConfig mapConfig = mapContainer.getMapConfig();
            MapConfig.EvictionPolicy evictionPolicy = mapConfig.getEvictionPolicy();
            Comparator comparator = null;
            if (evictionPolicy == MapConfig.EvictionPolicy.LRU) {
                comparator = new Comparator<AbstractRecord>() {
                    public int compare(AbstractRecord o1, AbstractRecord o2) {
                        return o1.getLastAccessTime().compareTo(o2.getLastAccessTime());
                    }
                };
            } else if (evictionPolicy == MapConfig.EvictionPolicy.LFU) {
                comparator = new Comparator<AbstractRecord>() {
                    public int compare(AbstractRecord o1, AbstractRecord o2) {
                        return o1.getHits().compareTo(o2.getHits());
                    }
                };
            }
            final int evictionPercentage = mapConfig.getEvictionPercentage();
            int memberCount = nodeEngine.getClusterService().getMembers().size();
            int targetSizePerPartition = -1;
            int maxPartitionSize = 0;
            final MaxSizeConfig.MaxSizePolicy maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
            if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_JVM) {
                maxPartitionSize = mapConfig.getMaxSizeConfig().getSize() * memberCount / nodeEngine.getPartitionService().getPartitionCount();
                targetSizePerPartition = Double.valueOf(maxPartitionSize * ((100 - evictionPercentage) / 100.0)).intValue();
            } else if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_PARTITION) {
                maxPartitionSize = mapConfig.getMaxSizeConfig().getSize();
                targetSizePerPartition = Double.valueOf(maxPartitionSize * ((100 - evictionPercentage) / 100.0)).intValue();
            }
            for (int i = 0; i < ExecutorConfig.DEFAULT_POOL_SIZE; i++) {
                nodeEngine.getExecutionService().execute("map-evict", new EvictRunner(i, mapConfig, targetSizePerPartition, comparator));
            }


        }

        private class EvictRunner implements Runnable {
            final int mod;
            String mapName;
            int targetSizePerPartition;
            Comparator comparator;
            MaxSizeConfig.MaxSizePolicy maxSizePolicy;
            int evictionPercentage;

            private EvictRunner(int mod, MapConfig mapConfig, int targetSizePerPartition, Comparator comparator) {
                this.mod = mod;
                mapName = mapConfig.getName();
                this.targetSizePerPartition = targetSizePerPartition;
                this.comparator = comparator;
                maxSizePolicy = mapConfig.getMaxSizeConfig().getMaxSizePolicy();
            }

            public void run() {
                for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
                    if ((i % ExecutorConfig.DEFAULT_POOL_SIZE) != mod) {
                        continue;
                    }
                    Address owner = nodeEngine.getPartitionService().getPartitionOwner(i);
                    if (nodeEngine.getThisAddress().equals(owner)) {
                        PartitionContainer pc = partitionContainers[i];
                        final RecordStore recordStore = pc.getRecordStore(mapName);
                        SortedSet sortedRecords = new TreeSet(comparator);
                        Set<Map.Entry<Data, Record>> recordEntries = recordStore.getRecords().entrySet();
                        for (Map.Entry<Data, Record> entry : recordEntries) {
                            sortedRecords.add(entry.getValue());
                        }
                        int evictSize = 0;
                        if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_JVM || maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_PARTITION) {
                            evictSize = sortedRecords.size() - targetSizePerPartition;
                        } else {
                            evictSize = sortedRecords.size() * evictionPercentage / 100;
                        }
                        if (evictSize == 0)
                            continue;
                        Set<Data> keySet = new HashSet();
                        Iterator iterator = sortedRecords.iterator();
                        while (iterator.hasNext() && evictSize-- > 0) {
                            Record rec = (Record) iterator.next();
                            keySet.add(rec.getKey());
                        }
                        ClearOperation clearOperation = new ClearOperation(mapName, keySet);
                        clearOperation.setNodeEngine(nodeEngine);
                        clearOperation.setServiceName(SERVICE_NAME);
                        clearOperation.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                        clearOperation.setPartitionId(i);
                        nodeEngine.getOperationService().runOperation(clearOperation);
                    }
                }
            }
        }

        private boolean checkLimits(MapContainer mapContainer) {
            MaxSizeConfig maxSizeConfig = mapContainer.getMapConfig().getMaxSizeConfig();
            MaxSizeConfig.MaxSizePolicy maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
            String mapName = mapContainer.getName();
            if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_JVM || maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_PARTITION) {
                int totalSize = 0;
                for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
                    Address owner = nodeEngine.getPartitionService().getPartitionOwner(i);
                    if (nodeEngine.getThisAddress().equals(owner)) {
                        int size = partitionContainers[i].getRecordStore(mapName).getRecords().size();
                        if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_PARTITION) {
                            if (size >= maxSizeConfig.getSize())
                                return true;
                        } else {
                            totalSize += size;
                        }
                    }
                }
                if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.PER_JVM)
                    return totalSize >= maxSizeConfig.getSize();
                else
                    return false;
            }
            if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE || maxSizePolicy == MaxSizeConfig.MaxSizePolicy.USED_HEAP_PERCENTAGE) {
                long total = Runtime.getRuntime().totalMemory();
                long used = (total - Runtime.getRuntime().freeMemory());
                if (maxSizePolicy == MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE) {
                    return maxSizeConfig.getSize() > (used / 1024 / 1024);
                } else {
                    return maxSizeConfig.getSize() > (used / total);
                }
            }
            return false;
        }
    }

    public QueryableEntrySet getQueryableEntrySet(String mapName) {
        List<Integer> memberPartitions = nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
        List<ConcurrentMap<Data, Record>> mlist = new ArrayList<ConcurrentMap<Data, Record>>();
        for (Integer partition : memberPartitions) {
            PartitionContainer container = getPartitionContainer(partition);
            RecordStore recordStore = container.getRecordStore(mapName);
            mlist.add(recordStore.getRecords());
        }
        return new QueryableEntrySet((SerializationServiceImpl) nodeEngine.getSerializationService(), mlist);
    }

    public void queryOnPartition(String mapName, Predicate predicate, int partitionId, QueryResult result) {
        PartitionContainer container = getPartitionContainer(partitionId);
        RecordStore recordStore = container.getRecordStore(mapName);
        ConcurrentMap<Data, Record> records = recordStore.getRecords();
        SerializationServiceImpl serializationService = (SerializationServiceImpl) nodeEngine.getSerializationService();
        for (Record record : records.values()) {
            Data key = record.getKey();
            QueryEntry queryEntry = new QueryEntry(serializationService, key, key, record.getValue());
            if (predicate.apply(queryEntry)) {
                result.add(new QueryResultEntryImpl(key, key, queryEntry.getValueData()));
            }
        }
    }


    public LocalMapStatsImpl createLocalMapStats(String mapName) {
        LocalMapStatsImpl localMapStats = new LocalMapStatsImpl();
        long now = Clock.currentTimeMillis();
        long ownedEntryCount = 0;
        long backupEntryCount = 0;
        long dirtyCount = 0;
        long ownedEntryMemoryCost = 0;
        long backupEntryMemoryCost = 0;
        long hits = 0;
        long lockedEntryCount = 0;

        MapContainer mapContainer = getMapContainer(mapName);
        int backupCount = mapContainer.getBackupCount();
        ClusterServiceImpl clusterService = (ClusterServiceImpl) nodeEngine.getClusterService();

        Address thisAddress = clusterService.getThisAddress();
        for (int i = 0; i < nodeEngine.getPartitionService().getPartitionCount(); i++) {
            PartitionInfo partitionInfo = nodeEngine.getPartitionService().getPartitionInfo(i);
            if (partitionInfo.getOwner().equals(thisAddress)) {
                PartitionContainer partitionContainer = getPartitionContainer(i);
                RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                ConcurrentMap<Data, Record> records = recordStore.getRecords();
                for (Record record : records.values()) {
                    RecordStats stats = record.getStats();
                    RecordState state = record.getState();
                    // there is map store and the record is dirty (waits to be stored)
                    if (mapContainer.getStore() != null && state.isDirty()) {
                        dirtyCount++;
                    }
                    ownedEntryCount++;
                    ownedEntryMemoryCost += record.getCost();
                    localMapStats.setLastAccessTime(stats.getLastAccessTime());
                    localMapStats.setLastUpdateTime(stats.getLastUpdateTime());
                    hits += stats.getHits();
                    if (recordStore.isLocked(record.getKey())) {
                        lockedEntryCount++;
                    }
                }
            } else {
                for (int j = 1; j < backupCount; j++) {
                    if (partitionInfo.getReplicaAddress(i).equals(thisAddress)) {
                        PartitionContainer partitionContainer = getPartitionContainer(i);
                        RecordStore recordStore = partitionContainer.getRecordStore(mapName);
                        ConcurrentMap<Data, Record> records = recordStore.getRecords();
                        for (Record record : records.values()) {
                            backupEntryCount++;
                            backupEntryMemoryCost += record.getCost();
                        }
                    }
                }
            }
        }

        localMapStats.setDirtyEntryCount(Util.zeroOrPositive(dirtyCount));
        localMapStats.setLockedEntryCount(Util.zeroOrPositive(lockedEntryCount));
        localMapStats.setHits(Util.zeroOrPositive(hits));
        localMapStats.setOwnedEntryCount(Util.zeroOrPositive(ownedEntryCount));
        localMapStats.setBackupEntryCount(Util.zeroOrPositive(backupEntryCount));
        localMapStats.setOwnedEntryMemoryCost(Util.zeroOrPositive(ownedEntryMemoryCost));
        localMapStats.setBackupEntryMemoryCost(Util.zeroOrPositive(backupEntryMemoryCost));
        localMapStats.setCreationTime(Util.zeroOrPositive(clusterService.getClusterTimeFor(mapContainer.getCreationTime())));
        return localMapStats;
    }


}
