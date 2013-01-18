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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapServiceConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.client.*;
import com.hazelcast.map.proxy.DataMapProxy;
import com.hazelcast.map.proxy.ObjectMapProxy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.MigrationEndpoint;
import com.hazelcast.partition.MigrationType;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.TransactionException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.ResponseHandlerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;

public class MapService implements ManagedService, MigrationAwareService, MembershipAwareService,
        TransactionalService, RemoteService, EventPublishingService<EventData, EntryListener>,
        ClientProtocolService {

    public final static String MAP_SERVICE_NAME = MapServiceConfig.SERVICE_NAME;

    private final ILogger logger;
    private final AtomicLong counter = new AtomicLong(new Random().nextLong());
    private final PartitionContainer[] partitionContainers;
    private final NodeEngineImpl nodeEngine;
    private final ConcurrentMap<String, MapInfo> mapInfos = new ConcurrentHashMap<String, MapInfo>();
    private final Map<Command, ClientCommandHandler> commandHandlers = new HashMap<Command, ClientCommandHandler>();
    private final ConcurrentMap<ListenerKey, String> eventRegistrations;


    public MapService(final NodeEngine nodeEngine) {
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
        this.logger = nodeEngine.getLogger(MapService.class.getName());
        partitionContainers = new PartitionContainer[nodeEngine.getPartitionCount()];
        eventRegistrations = new ConcurrentHashMap<ListenerKey, String>();
    }

    public void init(final NodeEngine nodeEngine, Properties properties) {
        int partitionCount = nodeEngine.getPartitionCount();
        for (int i = 0; i < partitionCount; i++) {
            PartitionInfo partition = nodeEngine.getPartitionInfo(i);
            partitionContainers[i] = new PartitionContainer(this, partition);
        }
        nodeEngine.getExecutionService().scheduleAtFixedRate(new MapEvictTask(), 10, 1, TimeUnit.SECONDS);
        registerClientOperationHandlers();
    }

    public MapInfo getMapInfo(String mapName) {
        MapInfo mapInfo = mapInfos.get(mapName);
        if (mapInfo == null) {
            mapInfo = new MapInfo(mapName, nodeEngine.getConfig().getMapConfig(mapName));
            mapInfos.put(mapName, mapInfo);
        }
        return mapInfo;
    }

    private void registerClientOperationHandlers() {
        registerHandler(Command.MGET, new MapGetHandler(this));
        registerHandler(Command.MSIZE, new MapSizeHandler(this));
        registerHandler(Command.MGETALL, new MapGetAllHandler(this));
        registerHandler(Command.MPUT, new MapPutHandler(this));
        registerHandler(Command.MTRYPUT, new MapTryPutHandler(this));
        registerHandler(Command.MSET, new MapSetHandler(this));
        registerHandler(Command.MPUTTRANSIENT, new MapPutTransientHandler(this));
        registerHandler(Command.MLOCK, new MapLockHandler(this));
        registerHandler(Command.MTRYLOCK, new MapLockHandler(this));
        registerHandler(Command.MTRYREMOVE, new MapTryRemoveHandler(this));
        registerHandler(Command.MISLOCKED, new MapIsLockedHandler(this));
        registerHandler(Command.MUNLOCK, new MapUnlockHandler(this));
        registerHandler(Command.MPUTALL, new MapPutAllHandler(this));
        registerHandler(Command.MREMOVE, new MapRemoveHandler(this));
        registerHandler(Command.MCONTAINSKEY, new MapContainsKeyHandler(this));
        registerHandler(Command.MCONTAINSVALUE, new MapContainsValueHandler(this));
        registerHandler(Command.MPUTIFABSENT, new MapPutIfAbsentHandler(this));
        registerHandler(Command.MREMOVEIFSAME, new MapRemoveIfSameHandler(this));
        registerHandler(Command.MREPLACEIFNOTNULL, new MapReplaceIfNotNullHandler(this));
        registerHandler(Command.MREPLACEIFSAME, new MapReplaceIfSameHandler(this));
        registerHandler(Command.MFLUSH, new MapFlushHandler(this));
        registerHandler(Command.MEVICT, new MapEvictHandler(this));
        registerHandler(Command.MENTRYSET, new MapEntrySetHandler(this));
        registerHandler(Command.KEYSET, new KeySetHandler(this));


        registerHandler(Command.MFORCEUNLOCK, new MapForceUnlockHandler(this));
    }

    void registerHandler(Command command, ClientCommandHandler handler) {
        commandHandlers.put(command, handler);
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

    public void beforeMigration(MigrationServiceEvent event) {
        // TODO: what if partition has transactions?
    }

    public Operation prepareMigrationOperation(MigrationServiceEvent event) {
        if (event.getPartitionId() < 0 || event.getPartitionId() >= nodeEngine.getPartitionCount()) {
            return null;
        }
        final PartitionContainer container = partitionContainers[event.getPartitionId()];
        return new MapMigrationOperation(container, event.getPartitionId(), event.getReplicaIndex(), false);
    }

    public void commitMigration(MigrationServiceEvent event) {
        logger.log(Level.FINEST, "Committing " + event);
        if (event.getMigrationEndpoint() == MigrationEndpoint.SOURCE) {
            if (event.getMigrationType() == MigrationType.MOVE) {
                clearPartitionData(event.getPartitionId());
            } else if (event.getMigrationType() == MigrationType.MOVE_COPY_BACK) {
                final PartitionContainer container = partitionContainers[event.getPartitionId()];
                for (DefaultRecordStore mapPartition : container.maps.values()) {
                    final MapConfig mapConfig = getMapInfo(mapPartition.name).getMapConfig();
                    if (mapConfig.getTotalBackupCount() < event.getCopyBackReplicaIndex()) {
                        mapPartition.clear();
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
        for (DefaultRecordStore mapPartition : container.maps.values()) {
            mapPartition.clear();
        }
        container.maps.clear();
        container.transactions.clear(); // TODO: not sure?
    }

    public Record createRecord(String name, Data dataKey, Object value, long ttl) {
        Record record = null;
        MapInfo mapInfo = getMapInfo(name);
        if (mapInfo.getMapConfig().getRecordType().equals("DATA")) {
            record = new DataRecord(dataKey, toData(value));
        } else if (mapInfo.getMapConfig().getRecordType().equals("OBJECT")) {
            record = new ObjectRecord(dataKey, toObject(value));
        }

        if (ttl <= 0 && mapInfo.getMapConfig().getTimeToLiveSeconds() > 0) {
            record.getState().updateTtlExpireTime(mapInfo.getMapConfig().getTimeToLiveSeconds());
            scheduleOperation(name, dataKey, mapInfo.getMapConfig().getTimeToLiveSeconds());
        }
        if (ttl > 0) {
            record.getState().updateTtlExpireTime(ttl);
            scheduleOperation(name, record.getKey(), ttl);
        }
        if (mapInfo.getMapConfig().getMaxIdleSeconds() > 0) {
            record.getState().updateIdleExpireTime(mapInfo.getMapConfig().getMaxIdleSeconds());
            scheduleOperation(name, dataKey, mapInfo.getMapConfig().getMaxIdleSeconds());
        }
        return record;
    }

    public void prepare(String txnId, int partitionId) throws TransactionException {
        System.out.println(nodeEngine.getThisAddress() + " MapService prepare " + txnId);
        PartitionContainer pc = partitionContainers[partitionId];
        TransactionLog txnLog = pc.getTransactionLog(txnId);
        int maxBackupCount = 1; //txnLog.getMaxBackupCount();
        try {
            nodeEngine.getOperationService().takeBackups(MAP_SERVICE_NAME, new MapTxnBackupPrepareOperation(txnLog), 0, partitionId,
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
            nodeEngine.getOperationService().takeBackups(MAP_SERVICE_NAME, new MapTxnBackupCommitOperation(txnId), 0, partitionId,
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
            nodeEngine.getOperationService().takeBackups(MAP_SERVICE_NAME, new MapTxnBackupRollbackOperation(txnId), 0, partitionId,
                    maxBackupCount, 60);
        } catch (Exception e) {
            throw new TransactionException(e);
        }
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public String getServiceName() {
        return MAP_SERVICE_NAME;
    }

    public DistributedObject createDistributedObject(Object objectId) {
        return new ObjectMapProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public DistributedObject createDistributedObjectForClient(Object objectId) {
        return new DataMapProxy(String.valueOf(objectId), this, nodeEngine);
    }

    public void destroyDistributedObject(Object objectId) {
        logger.log(Level.WARNING, "Destroying object: " + objectId);
    }

    public void memberAdded(final MembershipServiceEvent membershipEvent) {
    }

    public void memberRemoved(final MembershipServiceEvent membershipEvent) {
        // submit operations to partition threads to;
        // * release locks
        // * rollback transaction
        // * do not know ?
    }

    public void destroy() {
    }

    public Map<Command, ClientCommandHandler> getCommandMap() {
        return commandHandlers;
    }

    public void publishEvent(Address caller, String mapName, int eventType, Data dataKey, Data dataOldValue, Data dataValue) {
        Collection<EventRegistration> candidates = nodeEngine.getEventService().getRegistrations(MAP_SERVICE_NAME, mapName);
        Set<EventRegistration> registrationsWithValue = new HashSet<EventRegistration>();
        Set<EventRegistration> registrationsWithoutValue = new HashSet<EventRegistration>();
        Object key = toObject(dataKey);
        Object value = toObject(dataValue);

        for (EventRegistration candidate : candidates) {
            EntryEventFilter filter = (EntryEventFilter) candidate.getFilter();
            if (filter instanceof QueryEventFilter) {
                Object testValue = eventType == EntryEvent.TYPE_REMOVED ? toObject(dataOldValue):  value;
                QueryEventFilter qfilter = (QueryEventFilter) filter;
                if (qfilter.eval(new SimpleMapEntry(key, testValue))) {
                    if (filter.isIncludeValue()) {
                        registrationsWithValue.add(candidate);
                    } else {
                        registrationsWithoutValue.add(candidate);
                    }
                }

            } else if (filter.eval(dataKey)) {
                if (filter.isIncludeValue()) {
                    registrationsWithValue.add(candidate);
                } else {
                    registrationsWithoutValue.add(candidate);
                }
            }
        }
        if (registrationsWithValue.isEmpty() && registrationsWithoutValue.isEmpty())
            return;
        String source = nodeEngine.getNode().address.toString();
        EventData event = new EventData(source, caller, dataKey, dataValue, dataOldValue, eventType);

        nodeEngine.getEventService().publishEvent(MAP_SERVICE_NAME, registrationsWithValue, event);
        nodeEngine.getEventService().publishEvent(MAP_SERVICE_NAME, registrationsWithoutValue, event.cloneWithoutValues());
    }


    public void addEventListener(EntryListener entryListener, EventFilter eventFilter, String mapName) {
        EventRegistration registration = nodeEngine.getEventService().registerListener(MAP_SERVICE_NAME, mapName, eventFilter, entryListener);
        eventRegistrations.put(new ListenerKey(entryListener, ((EntryEventFilter) eventFilter).getKey()), registration.getId());
    }

    public void removeEventListener(EntryListener entryListener, String mapName, Object key) {
        String registrationId = eventRegistrations.get(new ListenerKey(entryListener, key));
        nodeEngine.getEventService().deregisterListener(MAP_SERVICE_NAME, mapName, registrationId);
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

    public void dispatchEvent(EventData eventData, EntryListener listener) {
        Member member = nodeEngine.getClusterService().getMember(eventData.getCaller());
        EntryEvent event = null;

        if (eventData.getDataOldValue() == null) {
            event = new EntryEvent(eventData.getSource(), member, eventData.getEventType(), toObject(eventData.getDataKey()), toObject(eventData.getDataNewValue()));
        } else {
            event = new EntryEvent(eventData.getSource(), member, eventData.getEventType(), toObject(eventData.getDataKey()), toObject(eventData.getDataOldValue()), toObject(eventData.getDataNewValue()));
        }

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
    }


    public void scheduleOperation(String mapName, Data key, long executeTime) {
        MapRecordStateOperation stateOperation = new MapRecordStateOperation(mapName, key);
        final MapRecordTask recordTask = new MapRecordTask(nodeEngine, stateOperation, nodeEngine.getPartitionId(key));
        nodeEngine.getExecutionService().schedule(recordTask, executeTime, TimeUnit.MILLISECONDS);
    }

    private class MapEvictTask implements Runnable {
        public void run() {
            for (MapInfo mapInfo : mapInfos.values()) {
                String evictionPolicy = mapInfo.getMapConfig().getEvictionPolicy();
                MaxSizeConfig maxSizeConfig = mapInfo.getMapConfig().getMaxSizeConfig();
                if (!evictionPolicy.equals("NONE") && maxSizeConfig.getSize() > 0) {
                    boolean check = checkLimits(mapInfo);
                    if (check) {
                        evictMap(mapInfo);
                    }
                }
            }
        }

        private void evictMap(MapInfo mapInfo) {
            Node node = nodeEngine.getNode();
            List recordList = new ArrayList();
            for (int i = 0; i < nodeEngine.getPartitionCount(); i++) {
                Address owner = node.partitionService.getPartitionOwner(i);
                if (node.address.equals(owner)) {
                    String mapName = mapInfo.getName();
                    PartitionContainer pc = partitionContainers[i];
                    RecordStore recordStore = pc.getRecordStore(mapName);
                    Set<Map.Entry<Data, Record>> recordEntries = recordStore.getRecords().entrySet();
                    for (Map.Entry<Data, Record> entry : recordEntries) {
                        recordList.add(entry.getValue());
                    }
                }
            }

            String evictionPolicy = mapInfo.getMapConfig().getEvictionPolicy();
            int evictSize = recordList.size() * mapInfo.getMapConfig().getEvictionPercentage() / 100;
            if (evictSize == 0)
                return;

            if (evictionPolicy.equals("LRU")) {
                Collections.sort(recordList, new Comparator<AbstractRecord>() {
                    public int compare(AbstractRecord o1, AbstractRecord o2) {
                        return o1.getLastAccessTime().compareTo(o2.getLastAccessTime());
                    }
                });
                for (Object record : recordList) {
                    AbstractRecord arec = (AbstractRecord) record;

                }
            } else if (evictionPolicy.equals("LFU")) {
                Collections.sort(recordList, new Comparator<AbstractRecord>() {
                    public int compare(AbstractRecord o1, AbstractRecord o2) {
                        return o1.getHits().compareTo(o2.getHits());
                    }
                });
            }
            Set<Data> keySet = new HashSet();
            for (int i = 0; i < evictSize; i++) {
                Record rec = (Record) recordList.get(i);
                keySet.add(rec.getKey());
            }
            for (Data key : keySet) {
                EvictOperation evictOperation = new EvictOperation(mapInfo.getName(), key, null);
                evictOperation.setPartitionId(nodeEngine.getPartitionId(key));
                evictOperation.setResponseHandler(ResponseHandlerFactory.createEmptyResponseHandler());
                evictOperation.setService(MapService.this);
                nodeEngine.getOperationService().runOperation(evictOperation);
            }
        }

        private boolean checkLimits(MapInfo mapInfo) {
            MaxSizeConfig maxSizeConfig = mapInfo.getMapConfig().getMaxSizeConfig();
            String mapName = mapInfo.getName();
            String maxSizePolicy = maxSizeConfig.getMaxSizePolicy();
            if (maxSizePolicy.equals("CLUSTER_WIDE") || maxSizePolicy.equals("PER_JVM") || maxSizePolicy.equals("PER_PARTITION")) {
                int totalSize = 0;
                Node node = nodeEngine.getNode();
                for (int i = 0; i < nodeEngine.getPartitionCount(); i++) {
                    Address owner = node.partitionService.getPartitionOwner(i);
                    if (node.address.equals(owner)) {
                        int size = partitionContainers[i].getRecordStore(mapName).getRecords().size();
                        if (maxSizePolicy.equals("PER_PARTITION")) {
                            if (size > maxSizeConfig.getSize())
                                return true;
                        } else {
                            totalSize += size;
                        }
                    }
                }
                if (maxSizePolicy.equals("CLUSTER_WIDE")) {
                    return totalSize * nodeEngine.getClusterService().getMembers().size() >= maxSizeConfig.getSize();
                } else if (maxSizePolicy.equals("PER_JVM"))
                    return totalSize > maxSizeConfig.getSize();
                else
                    return false;

            }

            if (maxSizePolicy.equals("USED_HEAP_SIZE") || maxSizePolicy.equals("USED_HEAP_PERCENTAGE")) {
                long total = Runtime.getRuntime().totalMemory();
                long used = (total - Runtime.getRuntime().freeMemory());
                if (maxSizePolicy.equals("USED_HEAP_SIZE")) {
                    return maxSizeConfig.getSize() > (used / 1024 / 1024);
                } else {
                    return maxSizeConfig.getSize() > (used / total);
                }
            }
            return false;
        }

    }
}
