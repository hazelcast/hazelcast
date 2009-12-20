/* 
 * Copyright (c) 2008-2009, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.cluster.AbstractRemotelyProcessable;
import com.hazelcast.config.ConfigProperty;
import static com.hazelcast.core.Instance.InstanceType;
import com.hazelcast.core.MapEntry;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Transaction;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TXN_TIMEOUT;

import com.hazelcast.impl.concurrentmap.AddMapIndex;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import static com.hazelcast.nio.IOUtil.*;
import com.hazelcast.nio.Packet;
import com.hazelcast.query.Index;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryContext;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

public final class ConcurrentMapManager extends BaseManager {
    static final int BLOCK_COUNT = ConfigProperty.CONCURRENT_MAP_BLOCK_COUNT.getInteger(271);
    static long GLOBAL_REMOVE_DELAY_MILLIS = ConfigProperty.REMOVE_DELAY_SECONDS.getLong() * 1000L;
    final Block[] blocks;
    final ConcurrentMap<String, CMap> maps;
    final ConcurrentMap<String, LocallyOwnedMap> mapLocallyOwnedMaps;
    final ConcurrentMap<String, MapNearCache> mapCaches;
    final OrderedExecutionTask[] orderedExecutionTasks;
    final MapMigrator mapMigrator;
    long newRecordId = 0;

    ConcurrentMapManager(Node node) {
        super(node);
        blocks = new Block[BLOCK_COUNT];
        maps = new ConcurrentHashMap<String, CMap>(10);
        mapLocallyOwnedMaps = new ConcurrentHashMap<String, LocallyOwnedMap>(10);
        mapCaches = new ConcurrentHashMap<String, MapNearCache>(10);
        orderedExecutionTasks = new OrderedExecutionTask[BLOCK_COUNT];
        mapMigrator = new MapMigrator(this);
        for (int i = 0; i < BLOCK_COUNT; i++) {
            orderedExecutionTasks[i] = new OrderedExecutionTask();
        }
        node.clusterService.registerPeriodicRunnable(new Runnable() {
            public void run() {
                Collection<CMap> cmaps = maps.values();
                for (CMap cmap : cmaps) {
                    cmap.startRemove();
                    cmap.startEviction();
                    if (cmap.writeDelayMillis > 0) {
                        cmap.startAsyncStoreWrite();
                    }
                }
            }
        });
        node.clusterService.registerPeriodicRunnable(mapMigrator);
        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEnryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_IF_ABSENT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_SAME, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_MULTI, new PutMultiOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE, new RemoveOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_IF_SAME, new RemoveOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_ITEM, new RemoveItemOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_ADD, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE_MULTI, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_LOCK, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_LOCK, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_LOCK_RETURN_OLD, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_UNLOCK, new UnlockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_ENTRIES, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_VALUES, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS_ALL, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MIGRATE_RECORD, new MigrationOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_MULTI, new RemoveMultiOperationHander());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_LIST, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_SET, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_SIZE, new SizeOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS, new ContainsOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_VALUE, new ContainsValueOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCK_INFO, new BlockInfoOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCKS, new BlocksOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MIGRATION_COMPLETE, new MigrationCompleteOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_VALUE_COUNT, new ValueCountOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_INVALIDATE, new InvalidateOperationHandler());
    }

    public void reset() {
        maps.clear();
    }

    public void syncForDead(Address deadAddress) {
        mapMigrator.syncForDead(deadAddress);
    }

    public void syncForAdd() {
        mapMigrator.syncForAdd();
    }

    public int hashBlocks() {
        int hash = 1;
        for (int i = 0; i < BLOCK_COUNT; i++) {
            Block block = blocks[i];
            hash = (hash * 31) + ((block == null) ? 0 : block.hashCode());
        }
        return hash;
    }

    public static class InitialState extends AbstractRemotelyProcessable {
        List<MapState> lsMapStates = new ArrayList();

        public InitialState() {
        }

        public void createAndAddMapState(CMap cmap) {
            MapState mapState = new MapState(cmap.name);
            int indexCount = cmap.mapIndexes.size();
            for (int i = 0; i < indexCount; i++) {
                Index index = cmap.indexes[i];
                AddMapIndex mi = new AddMapIndex(cmap.name, index.getExpression(), index.isOrdered());
                mapState.addMapIndex(mi);
            }
            lsMapStates.add(mapState);
        }

        public void process() {
            FactoryImpl factory = getNode().factory;
            if (factory.node.active) {
                for (MapState mapState : lsMapStates) {
                    CMap cmap = factory.node.concurrentMapManager.getOrCreateMap(mapState.name);
                    for (AddMapIndex mapIndex : mapState.lsMapIndexes) {
                        cmap.addIndex(mapIndex.getExpression(), mapIndex.isOrdered());
                    }
                }
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(lsMapStates.size());
            for (MapState mapState : lsMapStates) {
                mapState.writeData(out);
            }
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                MapState mapState = new MapState();
                mapState.readData(in);
                lsMapStates.add(mapState);
            }
        }

        class MapState implements DataSerializable {
            String name;
            List<AddMapIndex> lsMapIndexes = new ArrayList();

            MapState() {
            }

            MapState(String name) {
                this.name = name;
            }

            void addMapIndex(AddMapIndex mapIndex) {
                lsMapIndexes.add(mapIndex);
            }

            public void writeData(DataOutput out) throws IOException {
                out.writeUTF(name);
                out.writeInt(lsMapIndexes.size());
                for (AddMapIndex mapIndex : lsMapIndexes) {
                    mapIndex.writeData(out);
                }
            }

            public void readData(DataInput in) throws IOException {
                name = in.readUTF();
                int size = in.readInt();
                for (int i = 0; i < size; i++) {
                    AddMapIndex mapIndex = new AddMapIndex();
                    mapIndex.readData(in);
                    lsMapIndexes.add(mapIndex);
                }
            }
        }
    }

    void backupRecord(final Record rec) {
        if (rec.getMultiValues() != null) {
            List<Data> values = rec.getMultiValues();
            int initialVersion = ((int) rec.getVersion() - values.size());
            int version = (initialVersion < 0) ? 0 : initialVersion;
            for (int i = 0; i < values.size(); i++) {
                Data value = values.get(i);
                Record record = rec.copy();
                record.setValue(value);
                record.setVersion(version++);
                MBackupOp backupOp = new MBackupOp();
                backupOp.backup(record);
            }
        } else {
            MBackupOp backupOp = new MBackupOp();
            backupOp.backup(rec);
        }
    }

    void migrateRecord(final CMap cmap, final Record rec) {
        if (!node.active || node.factory.restarted) return;
        MMigrate mmigrate = new MMigrate();
        if (cmap.isMultiMap()) {
            List<Data> values = rec.getMultiValues();
            if (values == null || values.size() == 0) {
                mmigrate.migrateMulti(rec, null);
            } else {
                for (Data value : values) {
                    mmigrate.migrateMulti(rec, value);
                }
            }
        } else {
            boolean migrated = mmigrate.migrate(rec);
            if (!migrated) {
                logger.log(Level.FINEST, "Migration failed " + rec.getKey());
            }
        }
    }

    class MLock extends MBackupAndMigrationAwareOp {
        Data oldValue = null;

        public boolean unlock(String name, Object key, long timeout) {
            boolean unlocked = booleanCall(CONCURRENT_MAP_UNLOCK, name, key, null, timeout, -1);
            if (unlocked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
            }
            return unlocked;
        }

        public boolean lock(String name, Object key, long timeout) {
            boolean locked = booleanCall(CONCURRENT_MAP_LOCK, name, key, null, timeout, -1);
            if (locked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
            }
            return locked;
        }

        public boolean lockAndReturnOld(String name, Object key, long timeout, long txnId) {
            oldValue = null;
            boolean locked = booleanCall(CONCURRENT_MAP_LOCK_RETURN_OLD, name, key, null, timeout,
                    -1);
            if (locked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
            }
            return locked;
        }

        @Override
        public void afterGettingResult(Request request) {
            if (request.operation == CONCURRENT_MAP_LOCK_RETURN_OLD) {
                oldValue = doTake(request.value);
            }
            super.afterGettingResult(request);
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            if (request.operation == CONCURRENT_MAP_LOCK_RETURN_OLD) {
                oldValue = doTake(packet.value);
            }
            super.handleNoneRedoResponse(packet);
        }
    }

    class MContainsKey extends MTargetAwareOp {

        public boolean containsEntry(String name, Object key, Object value) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, value, 0, -1);
        }

        public boolean containsKey(String name, Object key) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, null, 0, -1);
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MEvict extends MBackupAndMigrationAwareOp {
        public boolean evict(String name, Object key) {
            return evict(CONCURRENT_MAP_EVICT, name, key);
        }

        public boolean evictInternally(String name, Object key) {
            return evict(CONCURRENT_MAP_EVICT_INTERNAL, name, key);
        }

        public boolean evict(ClusterOperation operation, String name, Object key) {
            Data k = (key instanceof Data) ? (Data) key : toData(key);
            request.setLocal(operation, name, k, null, 0, -1, -1, thisAddress);
            request.setBooleanRequest();
            doOp();
            boolean result = getResultAsBoolean();
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_REMOVE);
            }
            return result;
        }

        @Override
        public void process() {
            setTarget();
            if (!thisAddress.equals(target)) {
                setResult(false);
            } else {
                prepareForBackup();
                doLocalOp();
            }
        }

        @Override
        public void doLocalOp() {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.evict(request);
            setResult(request.response);
        }
    }

    class MMigrate extends MBackupAwareOp {
        volatile int keysize = 0;

        public boolean migrateMulti(Record record, Data value) {
            copyRecordToRequest(record, request, true);
            request.value = value;
            request.operation = CONCURRENT_MAP_MIGRATE_RECORD;
            request.setBooleanRequest();
            doOp();
            boolean result = getResultAsBoolean();
            backup(CONCURRENT_MAP_BACKUP_PUT);
            return result;
        }

        public boolean migrate(Record record) {
            keysize = record.getKey().size();
            copyRecordToRequest(record, request, true);
            if (request.key == null) throw new RuntimeException("req.key is null " + request.redoCount);
            request.operation = CONCURRENT_MAP_MIGRATE_RECORD;
            request.setBooleanRequest();
            doOp();
            boolean result = getResultAsBoolean();
            backup(CONCURRENT_MAP_BACKUP_PUT);
            return result;
        }

        @Override
        public void setTarget() {
            target = getTarget(request.key);
            if (target == null) {
                Block block = blocks[(request.blockId)];
                if (block != null) {
                    target = block.getMigrationAddress();
                }
            }
        }
    }

    class MGetMapEntry extends MTargetAwareOp {
        public MapEntry get(String name, Object key) {
            CMap.CMapEntry mapEntry = (CMap.CMapEntry) objectCall(CONCURRENT_MAP_GET_MAP_ENTRY, name, key, null, 0, -1);
            if (mapEntry != null) {
                mapEntry.set(name, key);
            }
            return mapEntry;
        }
    }

    class MAddRemoveKeyListener extends MTargetAwareOp {
        public boolean addRemoveListener(boolean add, String name, Object key) {
            ClusterOperation operation = (add) ? ClusterOperation.ADD_LISTENER : ClusterOperation.REMOVE_LISTENER;
            return booleanCall(operation, name, key, null, -1, -1);
        }
    }

    class MGet extends MTargetAwareOp {
        Object key = null;

        public Object get(String name, Object key, long timeout) {
            this.key = key;
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (txn.has(name, key)) {
                    return txn.get(name, key);
                }
            }
            MapNearCache cache = mapCaches.get(name);
            if (cache != null) {
                Object value = cache.get(key);
                if (value != null) {
                    return value;
                }
            }
            LocallyOwnedMap locallyOwnedMap = mapLocallyOwnedMaps.get(name);
            if (locallyOwnedMap != null) {
                Object value = locallyOwnedMap.get(key);
                if (value != OBJECT_REDO) {
                    return value;
                }
            }
            Object value = objectCall(CONCURRENT_MAP_GET, name, key, null, timeout, -1);
            if (value instanceof AddressAwareException) {
                rethrowException(request.operation, (AddressAwareException) value);
            }
            return value;
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            Data value = packet.value;
            if (value != null && value.size() > 0) {
                CMap cmap = getOrCreateMap(request.name);
                MapNearCache cache = cmap.mapNearCache;
                if (cache != null) {
                    cache.put(this.key, request.key, packet.value);
                }
            }
            super.handleNoneRedoResponse(packet);
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MValueCount extends MTargetAwareOp {
        public Object count(String name, Object key, long timeout) {
            request.setLongRequest();
            return objectCall(CONCURRENT_MAP_VALUE_COUNT, name, key, null, timeout, -1);
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MRemoveItem extends MBackupAndMigrationAwareOp {

        public boolean removeItem(String name, Object key) {
            return removeItem(name, key, null);
        }

        public boolean removeItem(String name, Object key, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                try {
                    boolean locked;
                    if (!txn.has(name, key)) {
                        MLock mlock = new MLock();
                        locked = mlock
                                .lockAndReturnOld(name, key, DEFAULT_TXN_TIMEOUT, txn.getId());
                        if (!locked)
                            throwCME(key);
                        Object oldObject = null;
                        Data oldValue = mlock.oldValue;
                        if (oldValue != null) {
                            oldObject = threadContext.toObject(oldValue);
                        }
                        txn.attachRemoveOp(name, key, null, (oldObject == null));
                        return (oldObject != null);
                    } else {
                        return (txn.attachRemoveOp(name, key, null, false) != null);
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                return false;
            } else {
                boolean removed = booleanCall(CONCURRENT_MAP_REMOVE_ITEM, name, key, value, 0, -1);
                if (removed) {
                    backup(CONCURRENT_MAP_BACKUP_REMOVE);
                }
                return removed;
            }
        }
    }

    class MRemove extends MBackupAndMigrationAwareOp {

        public Object remove(String name, Object key, long timeout) {
            return txnalRemove(CONCURRENT_MAP_REMOVE, name, key, null, timeout);
        }

        public Object removeIfSame(String name, Object key, Object value, long timeout) {
            return txnalRemove(CONCURRENT_MAP_REMOVE_IF_SAME, name, key, value, timeout);
        }

        private Object txnalRemove(ClusterOperation operation, String name, Object key, Object value,
                                   long timeout) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                try {
                    if (!txn.has(name, key)) {
                        MLock mlock = new MLock();
                        boolean locked = mlock
                                .lockAndReturnOld(name, key, DEFAULT_TXN_TIMEOUT, txn.getId());
                        if (!locked)
                            throwCME(key);
                        Object oldObject = null;
                        Data oldValue = mlock.oldValue;
                        if (oldValue != null) {
                            oldObject = threadContext.toObject(oldValue);
                        }
                        txn.attachRemoveOp(name, key, value, (oldObject == null));
                        return oldObject;
                    } else {
                        return txn.attachRemoveOp(name, key, value, false);
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                return null;
            } else {
                Object oldValue = objectCall(operation, name, key, value, timeout, -1);
                if (oldValue != null) {
                    if (oldValue instanceof AddressAwareException) {
                        rethrowException(operation, (AddressAwareException) oldValue);
                    }
                    backup(CONCURRENT_MAP_BACKUP_REMOVE);
                }
                return oldValue;
            }
        }
    }

    public void destroy(String name) {
        maps.remove(name);
    }

    class MAdd extends MBackupAndMigrationAwareOp {
        boolean addToList(String name, Object value) {
//            Data key = ThreadContext.get().toData(value);
//            boolean result = booleanCall(CONCURRENT_MAP_ADD_TO_LIST, name, key, null, 0, -1);
//            backup(CONCURRENT_MAP_BACKUP_ADD);
//            return result;
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                txn.attachAddOp(name, value);
                return true;
            } else {
                Data key = ThreadContext.get().toData(value);
                boolean result = booleanCall(CONCURRENT_MAP_ADD_TO_LIST, name, key, null, 0, -1);
                backup(CONCURRENT_MAP_BACKUP_ADD);
                return result;
            }
        }

        boolean addToSet(String name, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, value)) {
                    MContainsKey containsKey = new MContainsKey();
                    if (!containsKey.containsKey(name, value)) {
                        txn.attachPutOp(name, value, null, true);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                Data key = ThreadContext.get().toData(value);
                boolean result = booleanCall(CONCURRENT_MAP_ADD_TO_SET, name, key, null, 0, -1);
                backup(CONCURRENT_MAP_BACKUP_ADD);
                return result;
            }
        }
    }

    class MBackup extends MTargetAwareOp {
        protected Address owner = null;
        protected int distance = 0;

        public void sendBackup(ClusterOperation operation, Address owner, boolean hardCopy,
                               int distance, Request reqBackup) {
            this.owner = owner;
            this.distance = distance;
            request.operation = operation;
            request.setFromRequest(reqBackup, hardCopy);
            request.operation = operation;
            request.caller = thisAddress;
            request.setBooleanRequest();
            doOp();
        }

        public void reset() {
            super.reset();
            owner = null;
            distance = 0;
        }

        @Override
        public void process() {
            MemberImpl targetMember = getNextMemberAfter(owner, true, distance);
            if (targetMember == null) {
                setResult(Boolean.TRUE);
                return;
            }
            target = targetMember.getAddress();
            if (target.equals(thisAddress)) {
                doLocalOp();
            } else {
                invoke();
            }
        }
    }

    class MPutMulti extends MBackupAndMigrationAwareOp {

        boolean put(String name, Object key, Object value) {
            boolean result = booleanCall(CONCURRENT_MAP_PUT_MULTI, name, key, value, 0, -1);
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_PUT);
            }
            return result;
        }
    }

    void setIndexValues(Request request, Object value) {
        CMap cmap = getMap(request.name);
        if (cmap != null) {
            int indexCount = cmap.mapIndexes.size();
            if (indexCount > 0) {
                Index[] indexes = cmap.indexes;
                byte[] indexTypes = cmap.indexTypes;
                long[] newIndexes = new long[indexCount];
                boolean typesNew = false;
                if (indexTypes == null) {
                    indexTypes = new byte[indexCount];
                    typesNew = true;
                }
                for (int i = 0; i < indexes.length; i++) {
                    Index index = indexes[i];
                    if (index != null) {
                        Object realValue = value;
                        if (realValue instanceof Data) {
                            realValue = toObject((Data) value);
                        }
                        newIndexes[i] = index.extractLongValue(realValue);
                        if (typesNew) {
                            indexTypes[i] = index.getIndexType();
                        }
                    }
                }
                if (typesNew) {
                    cmap.indexTypes = indexTypes;
                }
                request.setIndexes(newIndexes, indexTypes);
            }
        }
    }

    public static class MultiData implements DataSerializable {
        List<Data> lsData = null;

        public MultiData() {
        }

        public MultiData(Data d1, Data d2) {
            lsData = new ArrayList<Data>(2);
            lsData.add(d1);
            lsData.add(d2);
        }

        public void writeData(DataOutput out) throws IOException {
            int size = lsData.size();
            out.writeInt(size);
            for (int i = 0; i < size; i++) {
                Data d = lsData.get(i);
                d.writeData(out);
            }
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            lsData = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = new Data();
                data.readData(in);
                lsData.add(data);
            }
        }

        public int size() {
            return (lsData == null) ? 0 : lsData.size();
        }

        public List<Data> getAllData() {
            return lsData;
        }

        public Data getData(int index) {
            return (lsData == null) ? null : lsData.get(index);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("MultiData");
            sb.append("{size=").append(size());
            sb.append('}');
            return sb.toString();
        }
    }

    class MPut extends MBackupAndMigrationAwareOp {

        public boolean replace(String name, Object key, Object oldValue, Object newValue, long timeout) {
            Object result = txnalReplaceIfSame(CONCURRENT_MAP_REPLACE_IF_SAME, name, key, newValue, oldValue, timeout);
            return (result == Boolean.TRUE);
        }

        public Object replace(String name, Object key, Object value, long timeout, long ttl) {
            return txnalPut(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, name, key, value, timeout, ttl);
        }

        public Object putIfAbsent(String name, Object key, Object value, long timeout, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT_IF_ABSENT, name, key, value, timeout, ttl);
        }

        public Object put(String name, Object key, Object value, long timeout, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT, name, key, value, timeout, ttl);
        }

        private Object txnalReplaceIfSame(ClusterOperation operation, String name, Object key, Object newValue, Object expectedValue, long timeout) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndReturnOld(name, key, DEFAULT_TXN_TIMEOUT, txn.getId());
                    if (!locked)
                        throwCME(key);
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = toObject(oldValue);
                    }
                    if (oldObject == null) {
                        return Boolean.FALSE;
                    } else {
                        if (expectedValue.equals(oldValue)) {
                            txn.attachPutOp(name, key, newValue, false);
                            return Boolean.TRUE;
                        } else {
                            return Boolean.FALSE;
                        }
                    }
                } else {
                    if (expectedValue.equals(txn.get(name, key))) {
                        txn.attachPutOp(name, key, newValue, false);
                        return Boolean.TRUE;
                    } else {
                        return Boolean.FALSE;
                    }
                }
            } else {
                Data dataExpected = toData(expectedValue);
                Data dataNew = toData(newValue);
                setLocal(operation, name, key, new MultiData(dataExpected, dataNew), timeout, -1);
                request.longValue = (request.value == null) ? Integer.MIN_VALUE : dataNew.hashCode();
                setIndexValues(request, newValue);
                request.setBooleanRequest();
                doOp();
                Object returnObject = getResultAsBoolean();
                if (returnObject instanceof AddressAwareException) {
                    rethrowException(operation, (AddressAwareException) returnObject);
                }
                if (returnObject != Boolean.FALSE) {
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                }
                return returnObject;
            }
        }

        private Object txnalPut(ClusterOperation operation, String name, Object key, Object value, long timeout, long ttl) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndReturnOld(name, key, DEFAULT_TXN_TIMEOUT, txn.getId());
                    if (!locked)
                        throwCME(key);
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = toObject(oldValue);
                    }
                    txn.attachPutOp(name, key, value, (oldObject == null));
                    return threadContext.isClient() ? oldValue : oldObject;
                } else {
                    return txn.attachPutOp(name, key, value, false);
                }
            } else {
                setLocal(operation, name, key, value, timeout, ttl);
                request.longValue = (request.value == null) ? Integer.MIN_VALUE : request.value.hashCode();
                setIndexValues(request, value);
                request.setObjectRequest();
                doOp();
                Object returnObject = getResultAsObject();
                if (returnObject instanceof AddressAwareException) {
                    rethrowException(operation, (AddressAwareException) returnObject);
                }
                backup(CONCURRENT_MAP_BACKUP_PUT);
                return returnObject;
            }
        }
    }

    class MRemoveMulti extends MBackupAndMigrationAwareOp {

        boolean remove(String name, Object key, Object value) {
            boolean result = booleanCall(CONCURRENT_MAP_REMOVE_MULTI, name, key, value, 0, -1);
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_REMOVE_MULTI);
            }
            return result;
        }
    }

    abstract class MBackupAndMigrationAwareOp extends MBackupAwareOp {
        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MBackupOp extends MBackupAwareOp {
        public void backup(Record record) {
            copyRecordToRequest(record, request, true);
            doOp();
            getResultAsBoolean();
            target = thisAddress;
            backup(CONCURRENT_MAP_BACKUP_PUT);
        }

        @Override
        public void process() {
            prepareForBackup();
            setResult(Boolean.TRUE);
        }
    }

    abstract class MBackupAwareOp extends MTargetAwareOp {
        protected final MBackup[] backupOps = new MBackup[3];
        protected volatile int backupCount = 0;
        protected final Request reqBackup = new Request();

        protected void backup(ClusterOperation operation) {
            if (backupCount > 0) {
                for (int i = 0; i < backupCount; i++) {
                    int distance = i + 1;
                    MBackup backupOp = backupOps[i];
                    if (backupOp == null) {
                        backupOp = new MBackup();
                        backupOps[i] = backupOp;
                    }
                    backupOp.sendBackup(operation, target, (distance < backupCount), distance, reqBackup);
                }
                for (int i = 0; i < backupCount; i++) {
                    MBackup backupOp = backupOps[i];
                    backupOp.getResultAsBoolean();
                }
            }
        }

        void prepareForBackup() {
            reqBackup.reset();
            backupCount = 0;
            if (lsMembers.size() > 1) {
                CMap map = getOrCreateMap(request.name);
                backupCount = map.getBackupCount();
                backupCount = Math.min(backupCount, lsMembers.size());
                if (backupCount > 0) {
                    reqBackup.setFromRequest(request, true);
                }
            }
        }

        @Override
        public void process() {
            prepareForBackup();
            super.process();
        }

        @Override
        protected void setResult(Object obj) {
            if (reqBackup.local) {
                reqBackup.version = request.version;
                reqBackup.lockCount = request.lockCount;
                reqBackup.longValue = request.longValue;
            }
            super.setResult(obj);
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            handleRemoteResponse(packet);
            super.handleNoneRedoResponse(packet);
        }

        public void handleRemoteResponse(Packet packet) {
            reqBackup.local = false;
            reqBackup.version = packet.version;
            reqBackup.lockCount = packet.lockCount;
            reqBackup.longValue = packet.longValue;
        }
    }

    abstract class MMigrationAwareTargettedCall extends MigrationAwareTargettedCall {

        @Override
        public void process() {
            request.blockId = hashBlocks();
            super.process();
        }
    }

    abstract class MTargetAwareOp extends TargetAwareOp {

        @Override
        public void doOp() {
            target = null;
            super.doOp();
        }

        @Override
        public void setTarget() {
            setTargetBasedOnKey();
        }

        final void setTargetBasedOnKey() {
            if (target == null) {
                target = getTarget(request.key);
            }
        }
    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Record record) {
        checkServiceThread();
        fireMapEvent(mapListeners, name, eventType, record.getKey(), record.getValue(), record.getMapListeners());
    }

    public Address getKeyOwner(Data key) {
        return getTarget(key);
    }

    public class MContainsValue extends MultiCall {
        boolean contains = false;
        final String name;
        final Object value;

        public MContainsValue(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MGetContainsValue(target);
        }

        boolean onResponse(Object response) {
            if (response == Boolean.TRUE) {
                this.contains = true;
                return false;
            }
            return true;
        }

        void onCall() {
            contains = false;
        }

        Object returnResult() {
            return contains;
        }

        class MGetContainsValue extends MMigrationAwareTargettedCall {
            public MGetContainsValue(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_CONTAINS_VALUE, name, null, value, 0, -1);
                request.setBooleanRequest();
            }
        }
    }

    public class MultiMigrationComplete extends MultiCall {
        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MMigrationComplete(target);
        }

        boolean onResponse(Object response) {
            return true;
        }

        Object returnResult() {
            return Boolean.TRUE;
        }

        class MMigrationComplete extends TargetAwareOp {
            public MMigrationComplete(Address target) {
                this.target = target;
                request.reset();
                request.setBooleanRequest();
                setLocal(CONCURRENT_MAP_MIGRATION_COMPLETE, "migration-complete");
            }

            public void setTarget() {
            }

            @Override
            public void onDisconnect(final Address dead) {
                if (dead.equals(target)) {
                    removeCall(getId());
                    setResult(Boolean.TRUE);
                }
            }
        }
    }

    public class MSize extends MultiCall {
        int size = 0;
        final String name;

        public int getSize() {
            int size = (Integer) call();
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            if (txn != null) {
                size += txn.size(name);
            }
            return (size < 0) ? 0 : size;
        }

        public MSize(String name) {
            this.name = name;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MGetSize(target);
        }

        boolean onResponse(Object response) {
            size += ((Long) response).intValue();
            return true;
        }

        void onCall() {
            size = 0;
        }

        Object returnResult() {
            return size;
        }

        class MGetSize extends MMigrationAwareTargettedCall {
            public MGetSize(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_SIZE, name);
                request.setLongRequest();
            }
        }
    }

    public class MIterateLocal extends MGetEntries {

        public MIterateLocal(String name, Predicate predicate) {
            super(thisAddress, CONCURRENT_MAP_ITERATE_KEYS, name, predicate);
            doOp();
        }

        public Set iterate() {
            Entries entries = new Entries(request.name, request.operation);
            Pairs pairs = (Pairs) getResultAsObject();
            entries.addEntries(pairs);
            return entries;
        }

        @Override
        public Object getResult() {
            return getRedoAwareResult();
        }
    }

    public class MIterate extends MultiCall {
        Entries entries = null;

        final String name;
        final ClusterOperation operation;
        final Predicate predicate;

        public MIterate(ClusterOperation operation, String name, Predicate predicate) {
            this.name = name;
            this.operation = operation;
            this.predicate = predicate;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MGetEntries(target, operation, name, predicate);
        }

        void onCall() {
            entries = new Entries(name, operation);
        }

        boolean onResponse(Object response) {
            //If Caller Thread is client, then the response is in form of Data
            //We need to deserialize it here
            Pairs pairs = null;
            if (response instanceof Data) {
                pairs = (Pairs) toObject((Data) response);
            } else {
                pairs = (Pairs) response;
            }
            entries.addEntries(pairs);
            return true;
        }

        Object returnResult() {
            return entries;
        }
    }

    class MGetEntries extends MMigrationAwareTargettedCall {
        public MGetEntries(Address target, ClusterOperation operation, String name, Predicate predicate) {
            this.target = target;
            request.reset();
            setLocal(operation, name, null, predicate, -1, -1);
        }

        @Override
        public void onDisconnect(final Address dead) {
        }
    }

    Address getTarget(Data key) {
        checkServiceThread();
        int blockId = getBlockId(key);
        Block block = blocks[blockId];
        if (block == null) {
            if (isMaster() && !isSuperClient()) {
                block = getOrCreateBlock(blockId);
                block.setOwner(thisAddress);
            } else
                return null;
        }
        if (block.isMigrating()) {
            return null;
        }
        return block.getOwner();
    }

    @Override
    public boolean isMigrating(Request req) {
//        if (migrating && req.key != null) {
//            CMap cmap = getMap(req.name);
//            if (cmap != null && cmap.getRecord(req.key) != null) {
//                return false;
//            }
//        }
//        return migrating;
        return mapMigrator.isMigrating(req);
    }

    public int getBlockId(Data key) {
        int hash = key.hashCode();
        return Math.abs(hash) % BLOCK_COUNT;
    }

    public long newRecordId() {
        return newRecordId++;
    }

    Block getOrCreateBlock(Data key) {
        return getOrCreateBlock(getBlockId(key));
    }

    void evictAsync(final String name, final Data key) {
        executeLocally(new Runnable() {
            public void run() {
                try {
                    MEvict mEvict = new MEvict();
                    mEvict.evictInternally(name, key);
                } catch (Exception ignored) {
                }
            }
        });
    }

    Block getOrCreateBlock(int blockId) {
        checkServiceThread();
        Block block = blocks[blockId];
        if (block == null) {
            block = new Block(blockId, null);
            blocks[blockId] = block;
        }
        return block;
    }

    CMap getMap(String name) {
        return maps.get(name);
    }

    public CMap getOrCreateMap(String name) {
        checkServiceThread();
        CMap map = maps.get(name);
        if (map == null) {
            map = new CMap(this, name);
            maps.put(name, map);
        }
        return map;
    }

    void checkServiceThread() {
        if (Thread.currentThread() != node.serviceThread) {
            throw new Error("Only ServiceThread can access this method. " + Thread.currentThread());
        }
    }

    @Override
    void handleListenerRegisterations(boolean add, String name, Data key,
                                      Address address, boolean includeValue) {
        CMap cmap = getOrCreateMap(name);
        if (add) {
            cmap.addListener(key, address, includeValue);
        } else {
            cmap.removeListener(key, address);
        }
    }

    // master should call this method
    boolean sendBlockInfo(Block block, Address address) {
        return send("mapblock", CONCURRENT_MAP_BLOCK_INFO, block, address);
    }

    void printBlocks() {
        logger.log(Level.FINEST, "=========================================");
        for (int i = 0; i < BLOCK_COUNT; i++) {
            logger.log(Level.FINEST, String.valueOf(blocks[i]));
        }
        logger.log(Level.FINEST, "=========================================");
    }

    class MigrationCompleteOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            Address from = request.caller;
            logger.log(Level.FINEST, "Migration Complete from " + from + ", local=" + request.local);
            if (from == null) throw new RuntimeException("Migration origin is not known!");
            for (Block block : blocks) {
                if (block != null && from.equals(block.getOwner())) {
                    if (block.isMigrating()) {
                        block.setOwner(block.getMigrationAddress());
                        block.setMigrationAddress(null);
                    }
                }
            }
            request.response = Boolean.TRUE;
        }
    }

    class BlocksOperationHandler extends BlockInfoOperationHandler {

        @Override
        public void process(Packet packet) {
            Blocks blocks = (Blocks) toObject(packet.value);
            handleBlocks(blocks);
            packet.returnToContainer();
        }

        void handleBlocks(Blocks blocks) {
            List<Block> lsBlocks = blocks.lsBlocks;
            for (Block block : lsBlocks) {
                doBlockInfo(block);
            }
        }
    }

    class BlockInfoOperationHandler implements PacketProcessor {

        public void process(Packet packet) {
            Block blockInfo = (Block) toObject(packet.value);
            doBlockInfo(blockInfo);
            if (thisAddress.equals(blockInfo.getOwner()) && blockInfo.isMigrating()) {
                mapMigrator.migrateBlock(blockInfo);
            }
            packet.returnToContainer();
        }

        void doBlockInfo(Block blockInfo) {
            Block block = blocks[blockInfo.getBlockId()];
            if (block == null) {
                block = blockInfo;
                blocks[block.getBlockId()] = block;
            } else {
                if (thisAddress.equals(block.getOwner())) {
                    // I am already owner!
                    if (block.isMigrating()) {
                        if (!block.getMigrationAddress().equals(blockInfo.getMigrationAddress())) {
                            throw new RuntimeException();
                        }
                    } else {
                        if (blockInfo.getMigrationAddress() != null) {
                            // I am being told to migrate
                            block.setMigrationAddress(blockInfo.getMigrationAddress());
                        }
                    }
                } else {
                    block.setOwner(blockInfo.getOwner());
                    block.setMigrationAddress(blockInfo.getMigrationAddress());
                }
            }
            if (block.getOwner() != null && block.getOwner().equals(block.getMigrationAddress())) {
                block.setMigrationAddress(null);
            }
        }
    }

    private void copyRecordToRequest(Record record, Request request, boolean includeKeyValue) {
        request.name = record.getName();
        request.version = record.getVersion();
        request.blockId = record.getBlockId();
        request.lockThreadId = record.getLockThreadId();
        request.lockAddress = record.getLockAddress();
        request.lockCount = record.getLockCount();
        request.longValue = record.getCopyCount();
        if (includeKeyValue) {
            request.key = record.getKey();
            if (record.getValue() != null) {
                request.value = record.getValue();
            }
        }
        if (record.getIndexes() != null) {
            CMap cmap = getMap(record.getName());
            request.setIndexes(record.getIndexes(), cmap.indexTypes);
        }
    }

    boolean rightRemoteTarget(Packet packet) {
        boolean right = thisAddress.equals(getTarget(packet.key));
        if (!right) {
            // not the owner (at least not anymore)
            if (isMaster()) {
                Block block = getOrCreateBlock(packet.key);
                sendBlockInfo(block, packet.conn.getEndPoint());
            }
            sendRedoResponse(packet);
        }
        return right;
    }

    class SizeOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = (long) cmap.size();
        }
    }

    class BackupPacketProcessor extends AbstractOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Object value = cmap.backup(request);
            request.clearForResponse();
            request.response = value;
        }
    }

    class InvalidateOperationHandler implements PacketProcessor {

        public void process(Packet packet) {
            CMap cmap = getMap(packet.name);
            if (cmap != null) {
                MapNearCache mapNearCache = cmap.mapNearCache;
                if (mapNearCache != null) {
                    mapNearCache.invalidate(packet.key);
                }
            }
            packet.returnToContainer();
        }
    }

    abstract class MTargetAwareOperationHandler extends TargetAwareOperationHandler {
        boolean isRightRemoteTarget(Packet packet) {
            return rightRemoteTarget(packet);
        }
    }

    class RemoveItemOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.removeItem(request);
        }
    }

    class RemoveOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.remove(request);
        }
    }

    class RemoveMultiOperationHander extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.removeMulti(request);
        }
    }

    class PutMultiOperationHandler extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.putMulti(request);
        }
    }

    class PutOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.put(request);
        }
    }

    class AddOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.add(request);
        }
    }

    class GetMapEnryOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.getMapEntry(request);
        }
    }

    class GetOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request.key);
            if (record == null && cmap.loader != null) {
                executeAsync(request);
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Data value = cmap.get(request);
            request.clearForResponse();
            request.response = value;
        }

        public void afterExecute(Request request) {
            if (request.response == Boolean.FALSE) {
                request.response = OBJECT_REDO;
            } else {
                if (request.value != null) {
                    Record record = ensureRecord(request);
                    if (record.getValue() == null) {
                        record.setValue(request.value);
                    }
                    request.response = record.getValue();
                }
            }
            returnResponse(request);
        }
    }

    class ValueCountOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.valueCount(request.key);
        }
    }

    class ContainsOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.contains(request);
        }
    }

    class MigrationOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.own(request);
            request.response = Boolean.TRUE;
        }
    }

    class UnlockOperationHandler extends SchedulableOperationHandler {
        protected boolean shouldSchedule(Request request) {
            return false;
        }

        void doOperation(Request request) {
            boolean unlocked = true;
            Record record = recordExist(request);
            logger.log(Level.FINEST, request.operation + " unlocking " + record);
            if (record != null) {
                unlocked = record.unlock(request.lockThreadId, request.lockAddress);
                if (unlocked) {
                    record.setVersion(record.getVersion() + 1);
                    request.version = record.getVersion();
                    request.lockCount = record.getLockCount();
                    fireScheduledActions(record);
                }
                logger.log(Level.FINEST, unlocked + " now lock count " + record.getLockCount() + " lockthreadId " + record.getLockThreadId());
            }
            if (unlocked) {
                request.response = Boolean.TRUE;
            } else {
                request.response = Boolean.FALSE;
            }
        }
    }

    class LockOperationHandler extends SchedulableOperationHandler {
        protected void onNoTimeToSchedule(Request request) {
            request.value = null;
            request.response = Boolean.FALSE;
            returnResponse(request);
        }

        void doOperation(Request request) {
            Record rec = ensureRecord(request);
            if (request.operation == CONCURRENT_MAP_LOCK_RETURN_OLD) {
                request.value = rec.getValue();
            }
            rec.lock(request.lockThreadId, request.lockAddress);
            rec.setVersion(rec.getVersion() + 1);
            request.version = rec.getVersion();
            request.lockCount = rec.getLockCount();
            request.response = Boolean.TRUE;
        }
    }

    abstract class SchedulableOperationHandler extends MTargetAwareOperationHandler {

        protected boolean shouldSchedule(Request request) {
            return (!testLock(request));
        }

        protected void onNoTimeToSchedule(Request request) {
            request.response = null;
            request.value = null;
            returnResponse(request);
        }

        protected void schedule(Request request) {
            Record record = ensureRecord(request);
            request.scheduled = true;
            ScheduledAction scheduledAction = new ScheduledAction(request) {
                @Override
                public boolean consume() {
                    handle(request);
                    return true;
                }

                @Override
                public void onExpire() {
                    onNoTimeToSchedule(request);
                }
            };
            record.addScheduledAction(scheduledAction);
            node.clusterManager.registerScheduledAction(scheduledAction);
        }

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }
    }

    interface AsynchronousExecution {
        /**
         * @param request
         * @executorThread
         */
        void execute(Request request);

        /**
         * @param request
         * @serviceThread
         */
        void afterExecute(Request request);
    }

    void executeAsync(Request request) {
        OrderedExecutionTask orderedExecutionTask = orderedExecutionTasks[getBlockId(request.key)];
        int size = orderedExecutionTask.offer(request);
        if (size == 1) {
            executeLocally(orderedExecutionTask);
        }
    }

    abstract class StoreAwareOperationHandler extends SchedulableOperationHandler implements AsynchronousExecution {

        // executor thread
        public void execute(Request request) {
            CMap cmap = maps.get(request.name);
            if (request.operation == CONCURRENT_MAP_GET) {
                // load the entry
                Object value = cmap.loader.load(toObject(request.key));
                if (value != null) {
                    request.value = toData(value);
                }
            } else if (request.operation == CONCURRENT_MAP_PUT || request.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                //store the entry
                cmap.store.store(toObject(request.key), toObject(request.value));
            } else if (request.operation == CONCURRENT_MAP_REMOVE) {
                // remove the entry
                cmap.store.delete(toObject(request.key));
            }
        }

        protected boolean shouldExecuteAsync(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            return (cmap.writeDelayMillis == 0);
        }

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
                return;
            }
            if (shouldExecuteAsync(request)) {
                executeAsync(request);
                return;
            }
            doOperation(request);
            returnResponse(request);
        }

        //serviceThread
        public void afterExecute(Request request) {
            if (request.response == null) {
                doOperation(request);
            } else {
                request.value = (Data) request.response;
            }
            returnResponse(request);
        }
    }

    class ContainsValueOperationHandler extends ExecutorServiceOperationHandler {

        @Override
        public void process(Packet packet) {
            processMigrationAware(packet);
        }

        Runnable createRunnable(final Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            return new Runnable() {
                public void run() {
                    request.response = node.queryService.containsValue(cmap.name, request.value);
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            returnResponse(request);
                        }
                    });
                }
            };
        }
    }

    abstract class ExecutorServiceOperationHandler extends ResponsiveOperationHandler {
        public void process(Packet packet) {
            Request request = new Request();
            request.setFromPacket(packet);
            request.local = false;
            handle(request);
        }

        public void handle(final Request request) {
            executeLocally(createRunnable(request));
        }

        abstract Runnable createRunnable(Request request);
    }

    class QueryOperationHandler extends ExecutorServiceOperationHandler {

        Runnable createRunnable(Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            return new QueryTask(cmap, request);
        }

        class QueryTask implements Runnable {
            final CMap cmap;
            final Request request;

            QueryTask(CMap cmap, Request request) {
                this.cmap = cmap;
                this.request = request;
            }

            public void run() {
                Predicate predicate = null;
                if (request.value != null) {
                    predicate = (Predicate) toObject(request.value);
                }
                QueryContext queryContext = new QueryContext(cmap.name, predicate);
                node.queryService.query(queryContext);
                Set<MapEntry> results = queryContext.getResults(); 
                if (predicate != null) {
                    if (!queryContext.isStrong()) {
                        Iterator<MapEntry> it = results.iterator();
                        while (it.hasNext()) {
                            Record record = (Record) it.next();
                            if (record.isActive()) {
                                try {
                                    if (!predicate.apply(record.getRecordEntry())) {
                                        it.remove();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    it.remove();
                                }
                            } else {
                                it.remove();
                            }
                        }
                    }
                }
                createEntries(request, results);
                enqueueAndReturn(new Processable() {
                    public void process() {
                        returnResponse(request);
                    }
                });
            }
        }

        void createEntries(Request request, Collection<MapEntry> colRecords) {
            Pairs pairs = new Pairs();
            long now = System.currentTimeMillis();
            for (MapEntry mapEntry : colRecords) {
                Record record = (Record) mapEntry;
                if (record.isActive() && record.isValid(now)) {
                    if (record.getKey() == null || record.getKey().size() == 0) {
                        throw new RuntimeException("Key cannot be null or zero-size: " + record.getKey());
                    }
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
                        if (record.getValue() != null || request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                            pairs.addKeyValue(new KeyValue(record.getKey(), null));
                        } else if (record.getCopyCount() > 0) {
                            for (int i = 0; i < record.getCopyCount(); i++) {
                                pairs.addKeyValue(new KeyValue(record.getKey(), null));
                            }
                        } else if (record.getMultiValues() != null) {
                            int size = record.getMultiValues().size();
                            if (size > 0) {
                                if (request.operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                    pairs.addKeyValue(new KeyValue(record.getKey(), null));
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        Data value = record.getMultiValues().get(i);
                                        pairs.addKeyValue(new KeyValue(record.getKey(), value));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Data dataEntries = toData(pairs);
            request.longValue = pairs.size();
            request.response = dataEntries;
        }
    }

    abstract class AsyncMigrationAwareOperationHandler extends MigrationAwareOperationHandler {

        abstract void handleAsync(Request request);

        abstract boolean shouldExecuteAsync(Request request);

        @Override
        public void handle(Request request) {
            if (shouldExecuteAsync(request)) {
                handleAsync(request);
            } else {
                super.handle(request);
            }
        }
    }

    final void doContains(Request request) {
        CMap cmap = getOrCreateMap(request.name);
        request.response = cmap.contains(request);
    }

    Record recordExist(Request req) {
        CMap cmap = maps.get(req.name);
        if (cmap == null)
            return null;
        return cmap.getRecord(req.key);
    }

    Record ensureRecord(Request req) {
        checkServiceThread();
        CMap cmap = getOrCreateMap(req.name);
        Record record = cmap.getRecord(req.key);
        if (record == null) {
            record = cmap.createNewRecord(req.key, req.value);
            req.key = null;
            req.value = null;
        }
        return record;
    }

    final boolean testLock(Request req) {
        Record record = recordExist(req);
        return record == null || record.testLock(req.lockThreadId, req.lockAddress);
    }

    class OrderedExecutionTask implements Runnable, Processable {
        Queue<Request> qResponses = new ConcurrentLinkedQueue();
        Queue<Request> qRequests = new ConcurrentLinkedQueue();
        AtomicInteger offerSize = new AtomicInteger();

        int offer(Request request) {
            qRequests.offer(request);
            return offerSize.incrementAndGet();
        }

        public void run() {
            while (true) {
                final Request request = qRequests.poll();
                if (request != null) {
                    try {
                        AsynchronousExecution ae = (AsynchronousExecution) getPacketProcessor(request.operation);
                        ae.execute(request);
                    } catch (Exception e) {
                        logger.log(Level.FINEST, "Store throwed exception for " + request.operation, e);
                        request.response = toData(new AddressAwareException(e, thisAddress));
                    }
                    offerSize.decrementAndGet();
                    qResponses.offer(request);
                    enqueueAndReturn(OrderedExecutionTask.this);
                } else {
                    return;
                }
            }
        }

        public void process() {
            final Request request = qResponses.poll();
            if (request != null) {
                //store the entry
                AsynchronousExecution ae = (AsynchronousExecution) getPacketProcessor(request.operation);
                ae.afterExecute(request);
            }
        }
    }

    public void unlock(Record record) {
        record.setLockThreadId(-1);
        record.setLockCount(0);
        record.setLockAddress(null);
        fireScheduledActions(record);
    }

    public void fireScheduledActions(Record record) {
        checkServiceThread();
        if (record.getLockCount() == 0) {
            record.setLockThreadId(-1);
            record.setLockAddress(null);
            if (record.getScheduledActions() != null) {
                while (record.getScheduledActions().size() > 0) {
                    ScheduledAction sa = record.getScheduledActions().remove(0);
                    node.clusterManager.deregisterScheduledAction(sa);
                    if (!sa.expired()) {
                        sa.consume();
                        return;
                    }
                }
            }
        }
    }

    public void onDisconnect(Record record, Address deadAddress) {
        if (record == null || deadAddress == null) return;
        List<ScheduledAction> lsScheduledActions = record.getScheduledActions();
        if (lsScheduledActions != null) {
            if (lsScheduledActions.size() > 0) {
                Iterator<ScheduledAction> it = lsScheduledActions.iterator();
                while (it.hasNext()) {
                    ScheduledAction sa = it.next();
                    if (deadAddress.equals(sa.request.caller)) {
                        node.clusterManager.deregisterScheduledAction(sa);
                        sa.setValid(false);
                        it.remove();
                    }
                }
            }
        }
        if (record.getLockCount() > 0) {
            if (deadAddress.equals(record.getLockAddress())) {
                unlock(record);
            }
        }
    }

    public static class Blocks extends AbstractRemotelyProcessable {
        List<Block> lsBlocks = new ArrayList<Block>(BLOCK_COUNT);

        public void addBlock(Block block) {
            lsBlocks.add(block);
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            for (int i = 0; i < size; i++) {
                Block block = new Block();
                block.readData(in);
                addBlock(block);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            int size = lsBlocks.size();
            out.writeInt(size);
            for (Block block : lsBlocks) {
                block.writeData(out);
            }
        }

        public void process() {
            BlocksOperationHandler h = (BlocksOperationHandler)
                    getNode().clusterService.getPacketProcessor(CONCURRENT_MAP_BLOCKS);
            h.handleBlocks(Blocks.this);
        }
    }

    public class Entries implements Set {
        final String name;
        final List<Map.Entry> lsKeyValues = new ArrayList<Map.Entry>();
        final ClusterOperation operation;
        final boolean checkValue;

        public Entries(String name, ClusterOperation operation) {
            this.name = name;
            this.operation = operation;
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            this.checkValue = (InstanceType.MAP == getInstanceType(name)) &&
                    (operation == CONCURRENT_MAP_ITERATE_VALUES
                            || operation == CONCURRENT_MAP_ITERATE_ENTRIES);
            if (txn != null) {
                List<Map.Entry> entriesUnderTxn = txn.newEntries(name);
                if (entriesUnderTxn != null) {
                    lsKeyValues.addAll(entriesUnderTxn);
                }
            }
        }

        public boolean isEmpty() {
            return (size() == 0);
        }

        public int size() {
            return lsKeyValues.size();
        }

        public Iterator iterator() {
            return new EntryIterator(lsKeyValues.iterator());
        }

        public void addEntries(Pairs pairs) {
            if (pairs.lsKeyValues == null) return;
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            for (KeyValue entry : pairs.lsKeyValues) {
                if (txn != null) {
                    Object key = entry.getKey();
                    if (txn.has(name, key)) {
                        Object value = txn.get(name, key);
                        if (value != null) {
                            lsKeyValues.add(createSimpleEntry(node.factory, name, key, value));
                        }
                    } else {
                        entry.setName(node.factory, name);
                        lsKeyValues.add(entry);
                    }
                } else {
                    entry.setName(node.factory, name);
                    lsKeyValues.add(entry);
                }
            }
        }

        public List<Map.Entry> getKeyValues() {
            return lsKeyValues;
        }

        class EntryIterator implements Iterator {
            final Iterator<Map.Entry> it;
            Map.Entry entry = null;
            boolean calledHasNext = false;

            public EntryIterator(Iterator<Map.Entry> it) {
                super();
                this.it = it;
            }

            public boolean hasNext() {
                calledHasNext = true;
                if (!it.hasNext()) {
                    return false;
                }
                entry = it.next();
                if (checkValue && entry.getValue() == null) {
                    return hasNext();
                }
                return true;
            }

            public Object next() {
                if (!calledHasNext) {
                    hasNext();
                }
                calledHasNext = false;
                if (operation == CONCURRENT_MAP_ITERATE_KEYS
                        || operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                    return entry.getKey();
                } else if (operation == CONCURRENT_MAP_ITERATE_VALUES) {
                    return entry.getValue();
                } else if (operation == CONCURRENT_MAP_ITERATE_ENTRIES) {
                    return entry;
                } else throw new RuntimeException("Unknown iteration type " + operation);
            }

            public void remove() {
                if (getInstanceType(name) == InstanceType.MULTIMAP) {
                    if (operation == CONCURRENT_MAP_ITERATE_KEYS) {
                        ((MultiMap) node.factory.getOrCreateProxyByName(name)).remove(entry.getKey(), null);
                    } else {
                        ((MultiMap) node.factory.getOrCreateProxyByName(name)).remove(entry.getKey(), entry.getValue());
                    }
                } else {
                    ((FactoryImpl.IRemoveAwareProxy) node.factory.getOrCreateProxyByName(name)).removeKey(entry.getKey());
                }
                it.remove();
            }
        }

        public boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean addAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public void clear() {
            throw new UnsupportedOperationException();
        }

        public boolean contains(Object o) {
            Iterator it = iterator();
            while (it.hasNext()) {
                if (o.equals(it.next())) return true;
            }
            return false;
        }

        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        public boolean removeAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public boolean retainAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public Object[] toArray() {
            return toArray(null);
        }

        public Object[] toArray(Object[] a) {
            List values = new ArrayList();
            Iterator it = iterator();
            while (it.hasNext()) {
                Object obj = it.next();
                if (obj != null) {
                    values.add(obj);
                }
            }
            if (a == null) {
                return values.toArray();
            } else {
                return values.toArray(a);
            }
        }
    }
}
