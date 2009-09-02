/* 
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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
import com.hazelcast.collection.SortedHashMap;
import static com.hazelcast.collection.SortedHashMap.OrderingType;
import com.hazelcast.config.ConfigProperty;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.*;
import static com.hazelcast.core.Instance.InstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TXN_TIMEOUT;
import com.hazelcast.nio.*;
import static com.hazelcast.nio.BufferUtil.*;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Predicate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class ConcurrentMapManager extends BaseManager {
    private static final int BLOCK_COUNT = ConfigProperty.CONCURRENT_MAP_BLOCK_COUNT.getInteger(271);
    private final Block[] blocks;
    private final Map<String, CMap> maps;
    private final ConcurrentMap<String, LocallyOwnedMap> mapLocallyOwnedMaps;
    private final OrderedExecutionTask[] orderedExecutionTasks;
    private static long GLOBAL_REMOVE_DELAY_MILLIS = ConfigProperty.REMOVE_DELAY_SECONDS.getLong() * 1000L;
    private long newId = 0;

    ConcurrentMapManager(Node node) {
        super(node);
        blocks = new Block[BLOCK_COUNT];
        maps = new ConcurrentHashMap<String, CMap>(10);
        mapLocallyOwnedMaps = new ConcurrentHashMap<String, LocallyOwnedMap>(10);
        orderedExecutionTasks = new OrderedExecutionTask[BLOCK_COUNT];
        for (int i = 0; i < BLOCK_COUNT; i++) {
            orderedExecutionTasks[i] = new OrderedExecutionTask();
        }
        node.clusterService.registerPeriodicRunnable(new Runnable() {
            public void run() {
                Collection<CMap> cmaps = maps.values();
                for (CMap cmap : cmaps) {
                    cmap.startRemove();
                    if (cmap.ttl != 0) {
                        cmap.startEviction();
                    }
                    if (cmap.writeDelaySeconds > 0) {
                        cmap.startAsyncStoreWrite();
                    }
                }
            }
        });
        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEnryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_IF_ABSENT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, new PutOperationHandler());
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
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_ENTRIES, new QOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_VALUES, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS_ALL, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MIGRATE_RECORD, new MigrationOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_MULTI, new RemoveMultiOperationHander());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_LIST, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_SET, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_SIZE, new SizeOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS, new ContainsOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCK_INFO, new BlockInfoOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCKS, new BlocksOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MIGRATION_COMPLETE, new MigrationCompleteOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_VALUE_COUNT, new ValueCountOperationHandler());
    }

    public void reset() {
        maps.clear();
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress.equals(thisAddress)) return;
        for (Block block : blocks) {
            if (block != null) {
                if (deadAddress.equals(block.owner)) {
                    MemberImpl member = getNextMemberBeforeSync(block.owner, true, 1);
                    if (member == null) {
                        if (!isSuperClient()) {
                            block.owner = thisAddress;
                        } else {
                            block.owner = null;
                        }
                    } else {
                        if (!deadAddress.equals(member.getAddress())) {
                            block.owner = member.getAddress();
                        } else {
                            block.owner = null;
                        }
                    }
                }
                if (block.migrationAddress != null) {
                    if (deadAddress.equals(block.migrationAddress)) {
                        MemberImpl member = getNextMemberBeforeSync(block.migrationAddress, true, 1);
                        if (member == null) {
                            if (!isSuperClient()) {
                                block.migrationAddress = thisAddress;
                            } else {
                                block.migrationAddress = null;
                            }
                        } else {
                            if (!deadAddress.equals(member.getAddress())) {
                                block.migrationAddress = member.getAddress();
                            } else {
                                block.migrationAddress = null;
                            }
                        }
                    }
                }
            }
        }
        Collection<CMap> cmaps = maps.values();
        for (CMap map : cmaps) {
            Collection<Record> records = map.mapRecords.values();
            for (Record record : records) {
                onDisconnect(record, deadAddress);
            }
        }
        if (isMaster() && isSuperClient()) {
            syncForAdd();
        } else {
            doResetRecords();
        }
    }

    public void syncForAdd() {
        if (isMaster()) {
            // make sue that all blocks are actually created
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block == null) {
                    getOrCreateBlock(i);
                }
            }

            List<Block> lsBlocksToRedistribute = new ArrayList<Block>();
            Map<Address, Integer> addressBlocks = new HashMap<Address, Integer>();
            int storageEnabledMemberCount = 0;
            for (MemberImpl member : lsMembers) {
                if (!member.isSuperClient()) {
                    addressBlocks.put(member.getAddress(), 0);
                    storageEnabledMemberCount++;
                }
            }
            if (storageEnabledMemberCount == 0)
                return;


            int aveBlockOwnCount = BLOCK_COUNT / (storageEnabledMemberCount);
            for (Block block : blocks) {
                if (block.owner == null) {
                    lsBlocksToRedistribute.add(block);
                } else {
                    if (!block.isMigrating()) {
                        Integer countInt = addressBlocks.get(block.owner);
                        int count = (countInt == null) ? 0 : countInt;
                        if (count >= aveBlockOwnCount) {
                            lsBlocksToRedistribute.add(block);
                        } else {
                            count++;
                            addressBlocks.put(block.owner, count);
                        }
                    }
                }
            }

            Set<Address> allAddress = addressBlocks.keySet();
            setNewMembers:
            for (Address address : allAddress) {
                Integer countInt = addressBlocks.get(address);
                int count = (countInt == null) ? 0 : countInt;
                while (count < aveBlockOwnCount) {
                    if (lsBlocksToRedistribute.size() > 0) {
                        Block blockToMigrate = lsBlocksToRedistribute.remove(0);
                        if (blockToMigrate.owner == null) {
                            blockToMigrate.owner = address;
                        } else {
                            blockToMigrate.migrationAddress = address;
                            if (blockToMigrate.owner.equals(blockToMigrate.migrationAddress)) {
                                blockToMigrate.migrationAddress = null;
                            }
                        }
                        count++;
                    } else {
                        break setNewMembers;
                    }
                }
            }
            int addressIndex = 0;
            for (int i = 0; i < BLOCK_COUNT; i++) {
                Block block = blocks[i];
                if (block.owner == null) {
                    int index = addressIndex++ % addressBlocks.size();
                    block.owner = (Address) addressBlocks.keySet().toArray()[index];
                }
            }

            Data dataAllBlocks = null;
            for (MemberImpl member : lsMembers) {
                if (!member.localMember()) {
                    if (dataAllBlocks == null) {
                        Blocks allBlocks = new Blocks();
                        for (Block block : blocks) {
                            allBlocks.addBlock(block);
                        }
                        dataAllBlocks = ThreadContext.get().toData(allBlocks);
                    }
                    send("blocks", CONCURRENT_MAP_BLOCKS, dataAllBlocks, member.getAddress());
                }
            }
            doResetRecords();
        }
    }

    public static class InitialState extends AbstractRemotelyProcessable {
        List<MapState> lsMapStates = new ArrayList();

        public InitialState() {
        }

        public void createAndAddMapState(CMap cmap) {
            MapState mapState = new MapState(cmap.name);
            Collection<Index<Record>> indexes = cmap.mapNamedIndexes.values();
            for (Index index : indexes){
                AddMapIndex mi = new AddMapIndex(cmap.name, index.indexName, index.expression,  index.ordered);
                mapState.addMapIndex(mi);
            }
            lsMapStates.add (mapState);
        }


        public void process() {
            FactoryImpl factory = getNode().factory;
            if (factory.active) {
                for (MapState mapState : lsMapStates) {
                    CMap cmap = factory.node.concurrentMapManager.getMap(mapState.name);
                    for (AddMapIndex mapIndex : mapState.lsMapIndexes) {
                        cmap.addIndex(mapIndex.indexName, mapIndex.expression, mapIndex.ordered);
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

    public static class AddMapIndex extends AbstractRemotelyProcessable {
        String mapName;
        String indexName;
        Expression expression;
        boolean ordered;

        public AddMapIndex() {
        }

        public AddMapIndex(String mapName, String indexName, Expression expression, boolean ordered) {
            this.mapName = mapName;
            this.indexName = indexName;
            this.expression = expression;
            this.ordered = ordered;
        }

        public void process() {
            CMap cmap = getNode().concurrentMapManager.getMap(mapName);
            cmap.addIndex(indexName, expression, ordered);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(mapName);
            out.writeUTF(indexName);
            BufferUtil.writeDataSerializable((DataSerializable) expression, out);
            out.writeBoolean(ordered);
        }

        public void readData(DataInput in) throws IOException {
            mapName = in.readUTF();
            indexName = in.readUTF();
            expression = (Expression) BufferUtil.readDataSerializable(in);
            ordered = in.readBoolean();
        }
    }

    void doResetRecords() {
        InitialState initialState = new InitialState();
        Collection<CMap> cmaps = maps.values();
        for (final CMap cmap : cmaps) {
            initialState.createAndAddMapState(cmap);
        }
        sendProcessableToAll(initialState, false);
        if (isSuperClient())
            return;
        cmaps = maps.values();
        for (final CMap cmap : cmaps) {
            final Object[] records = cmap.mapRecords.values().toArray();
            cmap.reset();
            for (Object recObj : records) {
                final Record rec = (Record) recObj;
                if (rec.isActive()) {
                    if (rec.getKey() == null || rec.getKey().size() == 0) {
                        throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                    }
                    executeLocally(new Runnable() {
                        public void run() {
                            MMigrate mmigrate = new MMigrate();
                            if (cmap.isMultiMap()) {
                                List<Data> values = rec.lsValues;
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
                    });
                }
            }
        }
        executeLocally(new Runnable() {
            public void run() {
                MultiMigrationComplete mmc = new MultiMigrationComplete();
                mmc.call();
                logger.log(Level.FINEST, "Migration ended!");
            }
        });
    }

    abstract class MBooleanOp extends MTargetAwareOp {
        @Override
        void handleNoneRedoResponse(Packet packet) {
            handleBooleanNoneRedoResponse(packet);
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
        void handleNoneRedoResponse(Packet packet) {
            if (request.operation == CONCURRENT_MAP_LOCK_RETURN_OLD) {
                oldValue = doTake(packet.value);
            }
            super.handleBooleanNoneRedoResponse(packet);
        }
    }

    class MContainsKey extends MBooleanOp {

        public boolean containsEntry(String name, Object key, Object value) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, value, 0, -1);
        }

        public boolean containsKey(String name, Object key) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, null, 0, -1);
        }
    }


    class MEvict extends MBackupAwareOp {
        public boolean evict(String name, Object key) {
            Data k = (key instanceof Data) ? (Data) key : toData(key);
            request.setLocal(CONCURRENT_MAP_EVICT, name, k, null, 0, -1, -1, thisAddress);
            doOp();
            boolean result = getResultAsBoolean();
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_REMOVE);
            }
            return result;
        }

        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
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
            CMap cmap = getMap(request.name);
            request.response = cmap.evict(request);
            setResult(request.response);
        }
    }


    class MMigrate extends MBackupAwareOp {

        public boolean migrateMulti(Record record, Data value) {
            copyRecordToRequest(record, request, true);
            request.value = value;
            request.operation = CONCURRENT_MAP_MIGRATE_RECORD;
            doOp();
            boolean result = getResultAsBoolean();
            backup(CONCURRENT_MAP_BACKUP_PUT);
            return result;
        }

        public boolean migrate(Record record) {
            copyRecordToRequest(record, request, true);
            if (request.key == null) throw new RuntimeException("req.key is null " + request.redoCount);
            request.operation = CONCURRENT_MAP_MIGRATE_RECORD;
            doOp();
            boolean result = getResultAsBoolean();
            backup(CONCURRENT_MAP_BACKUP_PUT);
            return result;
        }

        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
        }

        @Override
        public void setTarget() {
            target = getTarget(request.key);
            if (target == null) {
                Block block = blocks[(request.blockId)];
                if (block != null) {
                    target = block.migrationAddress;
                }
            }
        }
    }

    class MGetMapEntry extends MTargetAwareOp {
        public MapEntry get(String name, Object key) {
            CMapEntry mapEntry = (CMapEntry) objectCall(CONCURRENT_MAP_GET_MAP_ENTRY, name, key, null, 0, -1);
            mapEntry.set(node.factory.getName(), name, key);
            return mapEntry;
        }
    }


    class MAddRemoveKeyListener extends MBooleanOp {
        public boolean addRemoveListener(boolean add, String name, Object key) {
            ClusterOperation operation = (add) ? ClusterOperation.ADD_LISTENER : ClusterOperation.REMOVE_LISTENER;
            return booleanCall(operation, name, key, null, -1, -1);
        }
    }


    class MGet extends MTargetAwareOp {
        public Object get(String name, Object key, long timeout) {
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.txn;
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (txn.has(name, key)) {
                    return txn.get(name, key);
                }
            }
            LocallyOwnedMap locallyOwnedMap = getLocallyOwnedMap(name);
            if (locallyOwnedMap != null) {
                Object result = locallyOwnedMap.get(key);
                if (result != OBJECT_REDO) {
                    return result;
                }
            }
            Object value = objectCall(CONCURRENT_MAP_GET, name, key, null, timeout, -1);
            if (value instanceof AddressAwareException) {
                rethrowException(request.operation, (AddressAwareException) value);
            }
            return value;
        }
    }

    class MValueCount extends MTargetAwareOp {
        public Object count(String name, Object key, long timeout) {
            return objectCall(CONCURRENT_MAP_VALUE_COUNT, name, key, null, timeout, -1);
        }

        @Override
        void handleNoneRedoResponse(Packet packet) {
            super.handleLongNoneRedoResponse(packet);
        }
    }

    class MRemoveItem extends MBackupAndMigrationAwareOp {

        public boolean removeItem(String name, Object key) {
            return removeItem(name, key, null);
        }

        public boolean removeItem(String name, Object key, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.txn;
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

        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
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
            TransactionImpl txn = threadContext.txn;
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

    class MAdd extends MBackupAwareOp {
        boolean addToList(String name, Object value) {

            Data key = ThreadContext.get().toData(value);
            boolean result = booleanCall(CONCURRENT_MAP_ADD_TO_LIST, name, key, null, 0, -1);
            backup(CONCURRENT_MAP_BACKUP_ADD);
            return result;
        }

        boolean addToSet(String name, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.txn;
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

        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
        }
    }

    class MBackup extends MBooleanOp {
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

        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
        }
    }

    void setIndexValues(Request request, Object value) {
        CMap cmap = getMap(request.name);
        Index[] indexes = cmap.indexes;
        for (int i = 0; i < indexes.length; i++) {
            Index index = indexes[i];
            if (index != null) {
                request.setIndex(i, index.extractLongValue(value));
            }
        }
    }

    class MPut extends MBackupAndMigrationAwareOp {

        public Object replace(String name, Object key, Object value, long timeout) {
            return txnalPut(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, name, key, value, timeout);
        }

        public Object putIfAbsent(String name, Object key, Object value, long timeout) {
            return txnalPut(CONCURRENT_MAP_PUT_IF_ABSENT, name, key, value, timeout);
        }

        public Object put(String name, Object key, Object value, long timeout) {
            return txnalPut(CONCURRENT_MAP_PUT, name, key, value, timeout);
        }

        private Object txnalPut(ClusterOperation operation, String name, Object key, Object value, long timeout) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.txn;
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
                        txn.attachPutOp(name, key, value, (oldObject == null));
                        return oldObject;
                    } else {
                        return txn.attachPutOp(name, key, value, false);
                    }
                } catch (Exception e1) {
                    e1.printStackTrace();
                }
                return null;
            } else {
                setLocal(operation, name, key, value, timeout, -1);
                setIndexValues(request, value);
                doOp();
                Object oldValue = getResultAsObject();
                if (oldValue instanceof AddressAwareException) {
                    rethrowException(operation, (AddressAwareException) oldValue);
                }
                backup(CONCURRENT_MAP_BACKUP_PUT);
                return oldValue;
            }
        }
    }

    class MRemoveMulti extends MBackupAwareOp {

        boolean remove(String name, Object key, Object value) {
            boolean result = booleanCall(CONCURRENT_MAP_REMOVE_MULTI, name, key, value, 0, -1);
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_REMOVE_MULTI);
            }
            return result;
        }

        @Override
        void handleNoneRedoResponse(final Packet packet) {
            handleBooleanNoneRedoResponse(packet);
        }
    }

    abstract class MBackupAndMigrationAwareOp extends MBackupAwareOp {
        @Override
        public boolean isMigrationAware() {
            return true;
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
                CMap map = getMap(request.name);
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
        public void handleBooleanNoneRedoResponse(Packet packet) {
            handleRemoteResponse(packet);
            super.handleBooleanNoneRedoResponse(packet);
        }

        @Override
        public void handleObjectNoneRedoResponse(Packet packet) {
            handleRemoteResponse(packet);
            super.handleObjectNoneRedoResponse(packet);
        }

        public void handleRemoteResponse(Packet packet) {
            reqBackup.local = false;
            reqBackup.version = packet.version;
            reqBackup.lockCount = packet.lockCount;
            reqBackup.longValue = packet.longValue;
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
        fireMapEvent(mapListeners, name, eventType, record.getKey(), record.getValue(), record.mapListeners);
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

        class MGetContainsValue extends MigrationAwareTargettedCall {
            public MGetContainsValue(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_CONTAINS, name, null, value, 0, -1);
            }

            @Override
            void handleNoneRedoResponse(final Packet packet) {
                handleBooleanNoneRedoResponse(packet);
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

            @Override
            void handleNoneRedoResponse(final Packet packet) {
                handleBooleanNoneRedoResponse(packet);
            }
        }
    }


    public class MSize extends MultiCall {
        int size = 0;
        final String name;

        public int getSize() {
            int size = (Integer) call();
            TransactionImpl txn = ThreadContext.get().txn;
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

        class MGetSize extends MigrationAwareTargettedCall {
            public MGetSize(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_SIZE, name);
            }

            @Override
            void handleNoneRedoResponse(final Packet packet) {
                handleLongNoneRedoResponse(packet);
            }
        }
    }

    public class MIterate extends MultiCall {
        Entries entries = null;

        final String name;
        final ClusterOperation operation;
        final Predicate predicate;

        public MIterate(String name, Predicate predicate, ClusterOperation operation) {
            this.name = name;
            this.operation = operation;
            this.predicate = predicate;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MGetEntries(target);
        }

        void onCall() {
            entries = new Entries(name, operation);
        }

        boolean onResponse(Object response) {
            entries.addEntries((Pairs) response);
            return true;
        }

        Object returnResult() {
            return entries;
        }

        class MGetEntries extends MigrationAwareTargettedCall {
            public MGetEntries(Address target) {
                this.target = target;
                request.reset();
                setLocal(operation, name, null, predicate, -1, -1);
            }
        }
    }


    Address getTarget(Data key) {
        int blockId = getBlockId(key);
        Block block = blocks[blockId];
        if (block == null) {
            if (isMaster() && !isSuperClient()) {
                block = getOrCreateBlock(blockId);
                block.owner = thisAddress;
            } else
                return null;
        }
        if (block.isMigrating()) {
            return null;
        }
        return block.owner;
    }

    @Override
    public boolean isMigrating() {
        for (Block block : blocks) {
            if (block != null && block.isMigrating()) {
                return true;
            }
        }
        return false;
    }

    private int getBlockId(Data key) {
        int hash = key.hashCode();
        return Math.abs(hash) % BLOCK_COUNT;
    }

    Block getOrCreateBlock(Data key) {
        return getOrCreateBlock(getBlockId(key));
    }

    Block getOrCreateBlock(int blockId) {
        Block block = blocks[blockId];
        if (block == null) {
            block = new Block(blockId, null);
            blocks[blockId] = block;
        }
        return block;
    }

    LocallyOwnedMap getLocallyOwnedMap(String name) {
        return mapLocallyOwnedMaps.get(name);
    }

    CMap getMap(String name) {
        CMap map = maps.get(name);
        if (map == null) {
            map = new CMap(name);
            maps.put(name, map);
        }
        return map;
    }

    @Override
    void handleListenerRegisterations(boolean add, String name, Data key,
                                      Address address, boolean includeValue) {
        CMap cmap = getMap(name);
        if (add) {
            cmap.addListener(key, address, includeValue);
        } else {
            cmap.removeListener(key, address);
        }
    }

    // master should call this method
    void sendBlockInfo(Block block, Address address) {
        send("mapblock", CONCURRENT_MAP_BLOCK_INFO, block, address);
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
                if (block != null && from.equals(block.owner)) {
                    if (block.isMigrating()) {
                        block.owner = block.migrationAddress;
                        block.migrationAddress = null;
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
            doResetRecords();
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
            packet.returnToContainer();
        }

        void doBlockInfo(Block blockInfo) {
            Block block = blocks[blockInfo.blockId];
            if (block == null) {
                block = blockInfo;
                blocks[block.blockId] = block;
            } else {
                if (thisAddress.equals(block.owner)) {
                    // I am already owner!
                    if (block.isMigrating()) {
                        if (!block.migrationAddress.equals(blockInfo.migrationAddress)) {
                            throw new RuntimeException();
                        }
                    } else {
                        if (blockInfo.migrationAddress != null) {
                            // I am being told to migrate
                            block.migrationAddress = blockInfo.migrationAddress;
                        }
                    }
                } else {
                    block.owner = blockInfo.owner;
                    block.migrationAddress = blockInfo.migrationAddress;
                }
            }
            if (block.owner != null && block.owner.equals(block.migrationAddress)) {
                block.migrationAddress = null;
            }
        }
    }

    private void copyRecordToRequest(Record record, Request request, boolean includeKeyValue) {
        request.name = record.name;
        request.version = record.getVersion();
        request.blockId = record.blockId;
        request.lockThreadId = record.lockThreadId;
        request.lockAddress = record.lockAddress;
        request.lockCount = record.lockCount;
        request.longValue = record.copyCount;
        if (includeKeyValue) {
            request.key = doHardCopy(record.getKey());
            if (record.getValue() != null) {
                request.value = doHardCopy(record.getValue());
            }
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
            packet.setNoData();
            sendRedoResponse(packet);
        }
        return right;
    }

    class SizeOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = (long) cmap.size();
        }
    }

    class BackupPacketProcessor extends AbstractOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.backup(request);
        }
    }

    abstract class MTargetAwareOperationHandler extends TargetAwareOperationHandler {
        boolean isRightRemoteTarget(Packet packet) {
            return rightRemoteTarget(packet);
        }
    }

    class RemoveItemOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.removeItem(request);
        }
    }

    class RemoveOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.remove(request);
        }
    }


    class RemoveMultiOperationHander extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.removeMulti(request);
        }
    }

    class PutMultiOperationHandler extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.putMulti(request);
        }
    }

    class PutOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.put(request);
        }
    }

    class AddOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.add(request);
        }
    }

    class GetMapEnryOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.getMapEntry(request);
        }
    }


    class GetOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getMap(request.name);
            Record record = cmap.getRecord(request.key);
            if (record == null && cmap.loader != null) {
                executeAsync(request);
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }

        void doOperation(Request request) {

            CMap cmap = getMap(request.name);
            request.response = cmap.get(request);

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
                    request.response = doHardCopy(record.getValue());
                }
            }
            returnResponse(request);
        }
    }

    class ValueCountOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.valueCount(request.key);
        }
    }

    class ContainsOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            request.response = cmap.contains(request);
        }
    }

    class MigrationOperationHandler extends AbstractOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
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
                    request.lockCount = record.lockCount;
                    fireScheduledActions(record);
                }
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
                request.value = doHardCopy(rec.getValue());
            }
            rec.lock(request.lockThreadId, request.lockAddress);
            rec.setVersion(rec.getVersion() + 1);
            request.version = rec.getVersion();
            request.lockCount = rec.lockCount;
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
                Object value = cmap.loader.load(toObject(request.key, false));
                if (value != null) {
                    request.value = toData(value);
                }
            } else if (request.operation == CONCURRENT_MAP_PUT || request.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                //store the entry
                cmap.store.store(toObject(request.key, false), toObject(request.value, false));
            } else if (request.operation == CONCURRENT_MAP_REMOVE) {
                // remove the entry
                cmap.store.delete(toObject(request.key, false));
            }
        }

        protected boolean shouldExecuteAsync(Request request) {
            CMap cmap = getMap(request.name);
            return (cmap.writeDelaySeconds == 0);
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

    class QOperationHandler extends ResponsiveOperationHandler {
        public void process(Packet packet) {
            Request request = new Request();
            request.setFromPacket(packet);
            request.local = false;
            handle(request);
        }

        public void handle(Request request) {
            final CMap cmap = getMap(request.name);
            QueryTask queryTask = new QueryTask(cmap, request);
            executeLocally(queryTask);
        }

        class QueryTask implements Runnable {
            final CMap cmap;
            final Request request;

            QueryTask(CMap cmap, Request request) {
                this.cmap = cmap;
                this.request = request;
            }

            public void run() {
                Predicate predicate = (Predicate) toObject(request.value);
                Collection<Record> lsResults = new ArrayList();
                Collection<Record> records = node.queryService.query(cmap.mapRecords.values(), cmap.mapNamedIndexes, predicate);
                for (Record record : records) {
                    if (predicate == null || predicate.apply(record.getRecordEntry())) {
                        lsResults.add(record);
                    }
                }
                createEntries(request, lsResults);
                enqueueAndReturn(new Processable() {
                    public void process() {
                        returnResponse(request);
                    }
                });
            }
        }

        void createEntries(Request request, Collection<Record> colRecords) {
            CMap cmap = getMap(request.name);
            Pairs pairs = new Pairs();
            long now = System.currentTimeMillis();
            for (Record record : colRecords) {
                if (record.isValid(now)) {
                    if (record.getKey() == null || record.getKey().size() == 0) {
                        throw new RuntimeException("Key cannot be null or zero-size: " + record.getKey());
                    }
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        if (record.getValue() != null || request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                            pairs.addKeyValue(new KeyValue(record.getKey(), null));
                        } else if (record.copyCount > 0) {
                            for (int i = 0; i < record.copyCount; i++) {
                                pairs.addKeyValue(new KeyValue(record.getKey(), null));
                            }
                        } else if (record.lsValues != null) {
                            int size = record.lsValues.size();
                            if (size > 0) {
                                if (request.operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                    pairs.addKeyValue(new KeyValue(record.getKey(), null));
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        Data value = record.lsValues.get(i);
                                        pairs.addKeyValue(new KeyValue(record.getKey(), value));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (!record.isEvictable()) {
                        cmap.scheduleForEviction(record);
                    }
                }
            }
            Data dataEntries = toData(pairs);
            request.longValue = pairs.size();
            request.response = dataEntries;
        }
    }


    class QueryOperationHandler extends AsyncMigrationAwareOperationHandler {

        void doOperation(Request request) {
            CMap cmap = getMap(request.name);
            cmap.getEntries(request);
        }

        boolean shouldExecuteAsync(Request request) {
            return request.value != null;
        }

        void handleAsync(final Request request) {
            CMap cmap = getMap(request.name);
            final List<Record> lsRecords = cmap.getOwnedRecords();
            executeLocally(new Runnable() {
                public void run() {
                    final List<Record> lsResults = query(request, lsRecords);
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            createEntries(request, lsResults);
                            returnResponse(request);
                        }
                    });
                }
            });
        }


        public List<Record> query(Request request, List<Record> lsRecords) {
            Predicate predicate = (Predicate) toObject(request.value);
            List<Record> lsResults = new ArrayList<Record>(10);
            for (Record record : lsRecords) {
                if (record.isActive() && record.isValid()) {
                    MapEntry entry = record.getRecordEntry();
                    if (record.isActive() && entry.isValid()) {
                        try {
                            long version = entry.getVersion();
                            Object key = entry.getKey();
                            Object value = entry.getValue();
                            if (key != null && value != null) {
                                if (predicate.apply(entry)) {
                                    if (entry.isValid()) {
                                        lsResults.add(record);
                                    }
                                }
                            }
                        } catch (Throwable t) {
                        }
                    }
                }
            }
            return lsResults;
        }

        public void getEntries(Request request) {
            CMap cmap = getMap(request.name);
            Collection<Record> colRecords = cmap.mapRecords.values();
            createEntries(request, colRecords);
        }

        void createEntries(Request request, Collection<Record> colRecords) {
            CMap cmap = getMap(request.name);
            Pairs pairs = new Pairs();
            long now = System.currentTimeMillis();
            for (Record record : colRecords) {
                if (record.isValid(now)) {
                    if (record.getKey() == null || record.getKey().size() == 0) {
                        throw new RuntimeException("Key cannot be null or zero-size: " + record.getKey());
                    }
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        if (record.getValue() != null || request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                            pairs.addKeyValue(new KeyValue(record.getKey(), null));
                        } else if (record.copyCount > 0) {
                            for (int i = 0; i < record.copyCount; i++) {
                                pairs.addKeyValue(new KeyValue(record.getKey(), null));
                            }
                        } else if (record.lsValues != null) {
                            int size = record.lsValues.size();
                            if (size > 0) {
                                if (request.operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                    pairs.addKeyValue(new KeyValue(record.getKey(), null));
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        Data value = record.lsValues.get(i);
                                        pairs.addKeyValue(new KeyValue(record.getKey(), value));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (!record.isEvictable()) {
                        cmap.scheduleForEviction(record);
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
        CMap cmap = getMap(request.name);
        request.response = cmap.contains(request);
    }

    Record recordExist(Request req) {
        CMap cmap = maps.get(req.name);
        if (cmap == null)
            return null;
        return cmap.getRecord(req.key);
    }

    Record ensureRecord(Request req) {
        CMap cmap = getMap(req.name);
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

    class LocallyOwnedMap {
        final ConcurrentMap<Object, Record> mapCache = new ConcurrentHashMap<Object, Record>(1000);
        final private Queue<Record> localRecords = new ConcurrentLinkedQueue<Record>();

        public Object get(Object key) {
            processLocalRecords();
            Record record = mapCache.get(key);
            if (record == null) return OBJECT_REDO;
            else {
                if (record.isActive()) {
                    try {
                        long version = record.getVersion();
                        Object value = toObject(record.getValue(), false);
                        if (record.isActive() && version == record.getVersion()) {
                            record.setLastAccessed();
                            return value;
                        }
                    } catch (Throwable t) {
                        logger.log(Level.FINEST, "Exception when reading object ", t);
                        return OBJECT_REDO;
                    }
                } else {
                    //record is removed!
                    mapCache.remove(key);
                    return null;
                }
            }
            return OBJECT_REDO;
        }

        public void reset() {
            localRecords.clear();
            mapCache.clear();
        }

        private void processLocalRecords() {
            Record record = localRecords.poll();
            while (record != null) {
                if (record.isActive()) {
                    doPut(record);
                }
                record = localRecords.poll();
            }
        }

        public void doPut(Record record) {
            Object key = toObject(record.getKey(), false);
            mapCache.put(key, record);
        }

        public void offerToCache(Record record) {
            localRecords.offer(record);
        }
    }


    class CMap {
        final Set<Record> setRemovedRecords = new HashSet<Record>(1000);

        final SortedHashMap<Data, Record> mapRecords;

        final String name;

        final Map<Address, Boolean> mapListeners = new HashMap<Address, Boolean>(1);

        final int backupCount;

        final OrderingType evictionPolicy;

        final int maxSize;

        final float evictionRate;

        final long ttl; //ttl for entries

        final InstanceType instanceType;

        final MapLoader loader;

        final MapStore store;

        int writeDelaySeconds = -1;

        boolean evicting = false;

        int ownedEntryCount = 0;

        private Set<Record> setDirtyRecords = null;

        private final long removeDelayMillis;

        private Map<Integer, Set<Record>> mapValueIndex = new HashMap(1000);

        private final Map<String, Index<Record>> mapNamedIndexes = new HashMap(6);

        private final Index[] indexes = new Index[6];

        final LocallyOwnedMap locallyOwnedMap;

        public CMap(String name) {
            super();
            this.name = name;
            mapRecords = new SortedHashMap<Data, Record>(10000);
            MapConfig mapConfig = node.getConfig().getMapConfig(name.substring(2));
            this.backupCount = mapConfig.getBackupCount();
            ttl = mapConfig.getTimeToLiveSeconds() * 1000L;
            if ("LFU".equalsIgnoreCase(mapConfig.getEvictionPolicy())) {
                evictionPolicy = OrderingType.LFU;
            } else if ("LRU".equalsIgnoreCase(mapConfig.getEvictionPolicy())) {
                evictionPolicy = OrderingType.LRU;
            } else {
                evictionPolicy = OrderingType.NONE;
            }
            if (evictionPolicy == OrderingType.NONE) {
                maxSize = Integer.MAX_VALUE;
            } else {
                maxSize = (mapConfig.getMaxSize() == 0) ? MapConfig.DEFAULT_MAX_SIZE : mapConfig.getMaxSize();
            }
            evictionRate = mapConfig.getEvictionPercentage() / 100f;
            instanceType = getInstanceType(name);
            MapStoreConfig mapStoreConfig = mapConfig.getMapStoreConfig();
            MapStore storeTemp = null;
            MapLoader loaderTemp = null;
            if (mapStoreConfig != null) {
                if (mapStoreConfig.isEnabled()) {
                    String mapStoreClassName = mapStoreConfig.getClassName();
                    try {
                        Object storeInstance = Class.forName(mapStoreClassName, true, node.getConfig().getClassLoader()).newInstance();
                        if (storeInstance instanceof MapLoader) {
                            loaderTemp = (MapLoader) storeInstance;
                        }
                        if (storeInstance instanceof MapStore) {
                            storeTemp = (MapStore) storeInstance;
                        }
                        writeDelaySeconds = mapStoreConfig.getWriteDelaySeconds();
                        if (writeDelaySeconds > 0) {
                            setDirtyRecords = new HashSet<Record>(1000);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
            loader = loaderTemp;
            store = storeTemp;
            if (writeDelaySeconds > 0) {
                removeDelayMillis = GLOBAL_REMOVE_DELAY_MILLIS + (writeDelaySeconds * 1000L);
            } else {
                removeDelayMillis = GLOBAL_REMOVE_DELAY_MILLIS;
            }
            if (evictionPolicy == OrderingType.NONE && instanceType == InstanceType.MAP) {
                locallyOwnedMap = new LocallyOwnedMap();
                mapLocallyOwnedMaps.put(name, locallyOwnedMap);
            } else {
                locallyOwnedMap = null;
            }
        }

        public void addIndex(String indexName, Expression expression, boolean ordered) {
            if (!mapNamedIndexes.containsKey(indexName)) {
                Index index = new Index(indexName, expression, ordered);
                mapNamedIndexes.put(indexName, index);
                indexes[mapNamedIndexes.size() - 1] = index;
            }
        }

        public Record getRecord(Data key) {
            return mapRecords.get(key);
        }

        public int getBackupCount() {
            return backupCount;
        }

        public void own(Request req) {
            if (req.value == null) {
                req.value = new Data();
            }
            Record record = getRecord(req.key);
            if (record == null) {
                record = toRecord(req);
            }
            record.setVersion(req.version);
        }

        public boolean isMultiMap() {
            return (instanceType == InstanceType.MULTIMAP);
        }

        public boolean backup(Request req) {
            if (req.key == null || req.key.size() == 0) {
                throw new RuntimeException("Backup key size cannot be 0: " + req.key);
            }
            Record record = getRecord(req.key);
            if (record != null) {
                if (req.version > record.getVersion() + 1) {
                    Request reqCopy = new Request();
                    reqCopy.setFromRequest(req, true);
                    req.key = null;
                    req.value = null;
                    record.addBackupOp(new VersionedBackupOp(reqCopy));
                    return true;
                } else if (req.version <= record.getVersion()) {
                    return false;
                }
            }
            doBackup(req);
            if (record != null) {
                record.setVersion(req.version);
                record.runBackupOps();
            }
            return true;
        }


        public void doBackup(Request req) {
            if (req.key == null || req.key.size() == 0) {
                throw new RuntimeException("Backup key size cannot be zero! " + req.key);
            }
            if (req.operation == CONCURRENT_MAP_BACKUP_PUT) {
                Record rec = toRecord(req);
                if (rec.getVersion() == 0) {
                    rec.setVersion(req.version);
                }
            } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE) {
                Record record = getRecord(req.key);
                if (record != null) {
                    if (record.getValue() != null) {
                        record.getValue().setNoData();
                    }
                    if (record.lsValues != null) {
                        for (Data value : record.lsValues) {
                            value.setNoData();
                        }
                    }
                    if (record.copyCount > 0) {
                        record.decrementCopyCount();
                    }
                    record.setValue(null);
                    if (record.isRemovable()) {
                        removeAndPurgeRecord(record);
                    }
                }
            } else if (req.operation == CONCURRENT_MAP_BACKUP_LOCK) {
                Record rec = toRecord(req);
                if (rec.getVersion() == 0) {
                    rec.setVersion(req.version);
                }
            } else if (req.operation == CONCURRENT_MAP_BACKUP_ADD) {
                add(req);
            } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE_MULTI) {
                Record record = getRecord(req.key);
                if (record != null) {
                    if (req.value == null) {
                        removeAndPurgeRecord(record);
                    } else {
                        if (record.containsValue(req.value)) {
                            if (record.lsValues != null) {
                                Iterator<Data> itValues = record.lsValues.iterator();
                                while (itValues.hasNext()) {
                                    Data value = itValues.next();
                                    if (req.value.equals(value)) {
                                        itValues.remove();
                                        value.setNoData();
                                    }
                                }
                            }
                        }
                    }
                    if (record.isRemovable()) {
                        removeAndPurgeRecord(record);
                    }
                }
            } else {
                logger.log(Level.SEVERE, "Unknown backup operation " + req.operation);
            }
        }

        public int backupSize() {
            int size = 0;
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                Block block = blocks[record.blockId];
                if (!thisAddress.equals(block.owner)) {
                    size += record.valueCount();
                }
            }
            return size;
        }

        public int size() {
            long now = System.currentTimeMillis();
            int size = 0;
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                if (record.isValid(now)) {
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        size += record.valueCount();
                    }
                }
            }
//            for (int i = 0; i < BLOCK_COUNT; i++) {
//                System.out.println(blocks[i]);
//            }
//            System.out.println(size + " is size.. backup.size " + backupSize() + " ownedEntryCount:" + ownedEntryCount);
//            System.out.println("map size " + mapRecords.size());
            return size;
        }

        private List<Record> getOwnedRecords() {
            long now = System.currentTimeMillis();
            final List<Record> localRecords = new ArrayList<Record>(mapRecords.size() / 2);
            Collection<Record> records = mapRecords.values();
            for (Record record : records) {
                if (record.isValid(now)) {
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        localRecords.add(record);
                    }
                }
            }
            return localRecords;
        }

        public int valueCount(Data key) {
            long now = System.currentTimeMillis();
            int count = 0;
            Record record = mapRecords.get(key);
            if (record != null && record.isValid(now)) {
                count = record.valueCount();
            }
            return count;
        }

        public boolean contains(Request req) {
            Data key = req.key;
            Data value = req.value;
            if (key != null) {
                Record record = getRecord(req.key);
                if (record == null) {
                    return false;
                } else {
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        touch(record);
                        if (value == null) {
                            return record.valueCount() > 0;
                        } else {
                            return record.containsValue(value);
                        }
                    }
                }
            } else {
                if (!isMultiMap()) {
                    return searchValueIndex(value);
                } else {
                    Collection<Record> records = mapRecords.values();
                    for (Record record : records) {
                        Block block = blocks[record.blockId];
                        if (thisAddress.equals(block.owner)) {
                            if (record.containsValue(value)) {
                                return true;
                            }
                        }
                    }
                }
            }
            return false;
        }

        public void getEntries(Request request) {
            Collection<Record> colRecords = mapRecords.values();
            Pairs pairs = new Pairs();
            long now = System.currentTimeMillis();
            for (Record record : colRecords) {
                if (record.isValid(now)) {
                    if (record.getKey() == null || record.getKey().size() == 0) {
                        throw new RuntimeException("Key cannot be null or zero-size: " + record.getKey());
                    }
                    Block block = blocks[record.blockId];
                    if (thisAddress.equals(block.owner)) {
                        if (record.getValue() != null || request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL) {
                            pairs.addKeyValue(new KeyValue(record.getKey(), null));
                        } else if (record.copyCount > 0) {
                            for (int i = 0; i < record.copyCount; i++) {
                                pairs.addKeyValue(new KeyValue(record.getKey(), null));
                            }
                        } else if (record.lsValues != null) {
                            int size = record.lsValues.size();
                            if (size > 0) {
                                if (request.operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                    pairs.addKeyValue(new KeyValue(record.getKey(), null));
                                } else {
                                    for (int i = 0; i < size; i++) {
                                        Data value = record.lsValues.get(i);
                                        pairs.addKeyValue(new KeyValue(record.getKey(), value));
                                    }
                                }
                            }
                        }
                    }
                } else {
                    if (!record.isEvictable()) {
                        scheduleForEviction(record);
                    }
                }
            }
            Data dataEntries = toData(pairs);
            request.longValue = pairs.size();
            request.response = dataEntries;
        }

        public CMapEntry getMapEntry(Request req) {
            Record record = getRecord(req.key);
            if (record == null)
                return null;
            if (!record.isValid()) {
                return null;
            }
            return new CMapEntry(record.getCost(), record.expirationTime, record.getLastAccessTime(), record.getLastUpdateTime(),
                    record.getCreationTime(), record.getVersion(), record.getHits(), true);
        }

        public Data get(Request req) {
            Record record = getRecord(req.key);
            if (record == null)
                return null;
            if (!record.isActive()) return null;
            if (!record.isValid()) {
                if (record.isEvictable()) {
                    scheduleForEviction(record);
                    return null;
                }
            }
            if (req.local && locallyOwnedMap != null) {
                locallyOwnedMap.offerToCache(record);
            }
            record.setLastAccessed();
            touch(record);
            Data data = record.getValue();
            Data returnValue = null;
            if (data != null) {
                returnValue = doHardCopy(data);
            } else {
                if (record.lsValues != null) {
                    Values values = new Values(record.lsValues);
                    returnValue = toData(values);
                }
            }
            if (returnValue != null) {
                req.key.setNoData();
                req.key = null;
            }
            return returnValue;
        }

        public boolean add(Request req) {
            Record record = getRecord(req.key);
            if (record == null) {
                record = createNewRecord(req.key, null);
                req.key = null;
            } else {
                if (req.operation == CONCURRENT_MAP_ADD_TO_SET) {
                    return false;
                }
            }
            record.setVersion(record.getVersion() + 1);
            record.incrementCopyCount();
            fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record);
            return true;
        }

        public boolean removeMulti(Request req) {
            Record record = getRecord(req.key);
            if (record == null) return false;
            boolean removed = false;
            if (req.value == null) {
                removed = true;
                markAsRemoved(record);
            } else {
                if (record.containsValue(req.value)) {
                    if (record.lsValues != null) {
                        Iterator<Data> itValues = record.lsValues.iterator();
                        while (itValues.hasNext()) {
                            Data value = itValues.next();
                            if (req.value.equals(value)) {
                                itValues.remove();
                                value.setNoData();
                                removed = true;
                            }
                        }
                    }
                }
            }
            if (removed) {
                record.setVersion(record.getVersion() + 1);
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record.getKey(), req.value, record.mapListeners);
                logger.log(Level.FINEST, record.getValue() + " RemoveMulti " + record.lsValues);
            }
            req.version = record.getVersion();
            return removed;
        }

        public boolean putMulti(Request req) {
            Record record = getRecord(req.key);
            boolean added = true;
            if (record == null) {
                record = createNewRecord(req.key, null);
                req.key = null;
            } else {
                if (record.containsValue(req.value)) {
                    added = false;
                }
            }
            if (added) {
                record.addValue(req.value);
                req.value = null;
                record.setVersion(record.getVersion() + 1);
                touch(record);
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record.getKey(), req.value, record.mapListeners);
            }
            logger.log(Level.FINEST, record.getValue() + " PutMulti " + record.lsValues);
            req.version = record.getVersion();
            return added;
        }

        public Data put(Request req) {
            if (req.value == null) {
                req.value = new Data();
            }
            if (req.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                Record record = recordExist(req);
                if (record != null && record.isActive() && record.getValue() != null) {
                    return doHardCopy(record.getValue());
                }
            } else if (req.operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL) {
                Record record = recordExist(req);
                if (record == null || !record.isActive() || record.getValue() == null) {
                    return null;
                }
            }
            Record record = getRecord(req.key);
            Data oldValue = null;
            if (record == null) {
                record = createNewRecord(req.key, req.value);
                req.key = null;
            } else {
                markAsActive(record);
                if (!record.isValid()) {
                    record.setExpirationTime(ttl);
                }
                oldValue = record.getValue();
                record.setValue(req.value);
                record.incrementVersion();
                touch(record);
                record.setLastUpdated();
                updateValueIndex(oldValue, record);
            }
            req.version = record.getVersion();
            req.longValue = record.copyCount;
            req.value = null;
            if (oldValue == null) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record);
            } else {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_UPDATED, record);
            }
            if (req.txnId != -1) {
                unlock(record);
            }
            updateIndexes(req, record);
            markAsDirty(record);
            return oldValue;
        }

        void updateIndexes(Request request, Record record) {
            int indexCount = request.indexCount;
            if (mapNamedIndexes.size() > indexCount) {
                 throw new RuntimeException(indexCount + " but expected " + mapNamedIndexes.size());
            }
            for (int i = 0; i < indexCount; i++) {
                Index index = indexes[i];
                long newValue = request.indexes[i];
                long oldValue = record.getAndSetIndex(i, newValue);
                if (oldValue == Long.MIN_VALUE) {
                    node.queryService.addNewIndex(index, newValue, record);
                } else {
                    if (oldValue != newValue) {
                        node.queryService.updateIndex(index, oldValue, newValue, record);
                    }
                }
            }
        }


        void garbage(Data data) {
            data.setNoData();
        }

        void startAsyncStoreWrite() {
            logger.log(Level.FINEST, "startAsyncStoreWrite " + setDirtyRecords.size());
            long now = System.currentTimeMillis();
            Iterator<Record> itDirtyRecords = setDirtyRecords.iterator();
            final Map<Data, Data> entriesToStore = new HashMap<Data, Data>();
            final Collection<Data> keysToDelete = new HashSet<Data>();

            while (itDirtyRecords.hasNext()) {
                Record dirtyRecord = itDirtyRecords.next();
                if (dirtyRecord.writeTime > now) {
                    if (dirtyRecord.getValue() != null) {
                        entriesToStore.put(doHardCopy(dirtyRecord.getKey()), doHardCopy(dirtyRecord.getValue()));
                    } else {
                        keysToDelete.add(doHardCopy(dirtyRecord.getKey()));
                    }
                    dirtyRecord.dirty = false;
                    itDirtyRecords.remove();
                }
            }
            final int entriesToStoreSize = entriesToStore.size();
            final int keysToDeleteSize = keysToDelete.size();
            if (entriesToStoreSize > 0 || keysToDeleteSize > 0) {
                executeLocally(new Runnable() {
                    public void run() {
                        if (keysToDeleteSize > 0) {
                            if (keysToDeleteSize == 1) {
                                Data key = keysToDelete.iterator().next();
                                store.delete(toObject(key));
                            } else {
                                Collection realKeys = new HashSet();
                                for (Data key : keysToDelete) {
                                    realKeys.add(toObject(key));
                                }
                                store.deleteAll(realKeys);
                            }
                        }
                        if (entriesToStoreSize > 0) {
                            Object keyFirst = null;
                            Object valueFirst = null;
                            Set<Map.Entry<Data, Data>> entries = entriesToStore.entrySet();
                            Map realEntries = new HashMap();
                            for (Map.Entry<Data, Data> entry : entries) {
                                Object key = toObject(entry.getKey());
                                Object value = toObject(entry.getValue());
                                realEntries.put(key, value);
                                if (keyFirst == null) {
                                    keyFirst = key;
                                    valueFirst = value;
                                }
                            }
                            if (entriesToStoreSize == 1) {
                                store.store(keyFirst, valueFirst);
                            } else {
                                store.storeAll(realEntries);
                            }
                        }
                    }
                });
            }
        }

        void startRemove() {
            long now = System.currentTimeMillis();
            Iterator<Record> itRemovedRecords = setRemovedRecords.iterator();
            while (itRemovedRecords.hasNext()) {
                Record record = itRemovedRecords.next();
                if (record.isActive()) {
                    itRemovedRecords.remove();
                } else if (shouldRemove(record, now)) {
                    itRemovedRecords.remove();
                    removeAndPurgeRecord(record);
                }
            }
        }

        void startEviction() {
            if (evicting) return;
            List<Data> lsKeysToEvict = null;
            if (evictionPolicy == OrderingType.NONE) {
                if (ttl != 0) {
                    long now = System.currentTimeMillis();
                    Collection<Record> values = mapRecords.values();
                    for (Record record : values) {
                        if (!record.isValid(now)) {
                            if (record.isEvictable()) {
                                if (lsKeysToEvict == null) {
                                    lsKeysToEvict = new ArrayList<Data>(100);
                                }
                                lsKeysToEvict.add(doHardCopy(record.getKey()));
                            }
                        } else {
                            break;
                        }
                    }
                }
            } else {
                Collection<Record> values = mapRecords.values();
                int numberOfRecordsToEvict = (int) (ownedEntryCount * evictionRate);
                int evictedCount = 0;
                for (Record record : values) {
                    if (record.isEvictable()) {
                        if (lsKeysToEvict == null) {
                            lsKeysToEvict = new ArrayList<Data>(numberOfRecordsToEvict);
                        }
                        lsKeysToEvict.add(doHardCopy(record.getKey()));
                        if (++evictedCount >= numberOfRecordsToEvict) {
                            break;
                        }
                    }
                }
            }
            if (lsKeysToEvict != null && lsKeysToEvict.size() > 1) {
//                System.out.println(ownedEntryCount + " stating eviction..." + lsKeysToEvict.size());
                evicting = true;
                int latchCount = lsKeysToEvict.size();
                final CountDownLatch countDownLatchEvictionStart = new CountDownLatch(1);
                final CountDownLatch countDownLatchEvictionEnd = new CountDownLatch(latchCount);

                executeLocally(new Runnable() {
                    public void run() {
                        try {
                            countDownLatchEvictionStart.countDown();
                            countDownLatchEvictionEnd.await(60, TimeUnit.SECONDS);
                            enqueueAndReturn(new Processable() {
                                public void process() {
                                    evicting = false;
                                }
                            });
                        } catch (Exception ignored) {
                        }
                    }
                });
                for (final Data key : lsKeysToEvict) {
                    executeLocally(new Runnable() {
                        public void run() {
                            try {
                                countDownLatchEvictionStart.await();
                                MEvict mEvict = new MEvict();
                                mEvict.evict(name, key);
                                countDownLatchEvictionEnd.countDown();
                            } catch (Exception ignored) {
                            }
                        }
                    });
                }
            }
        }

        final boolean evict(Request req) {
            Record record = getRecord(req.key);
            if (record != null && record.isEvictable()) {
                if (ownerForSure(record)) {
                    fireMapEvent(mapListeners, name, EntryEvent.TYPE_EVICTED, record.getKey(), record.getValue(), record.mapListeners);
                    removeAndPurgeRecord(record);
                    return true;
                }
            }
            return false;
        }

        final boolean ownerForSure(Record record) {
            Block block = blocks[record.blockId];
            return block != null && !block.isMigrating() && thisAddress.equals(block.owner);
        }

        public Record toRecord(Request req) {
            Record record = getRecord(req.key);
            if (record == null) {
                if (isMultiMap()) {
                    record = createNewRecord(req.key, null);
                    record.addValue(req.value);
                } else {
                    record = createNewRecord(req.key, req.value);
                }
                req.key = null;
            } else {
                if (req.value != null) {
                    if (record.getValue() != null) {
                        record.getValue().setNoData();
                    }
                    if (isMultiMap()) {
                        record.addValue(req.value);
                    } else {
                        record.setValue(req.value);
                    }
                }
            }
            req.value = null;
            record.copyCount = (int) req.longValue;
            if (req.lockCount > 0) {
                record.lockAddress = req.lockAddress;
                record.lockThreadId = req.lockThreadId;
                record.lockCount = req.lockCount;
            }
            return record;
        }

        public boolean removeItem(Request req) {
            Record record = mapRecords.get(req.key);
            req.key.setNoData();
            req.key = null;
            if (record == null) {
                return false;
            }
            if (req.txnId != -1) {
                unlock(record);
            }
            boolean removed = false;
            if (record.getCopyCount() > 0) {
                record.decrementCopyCount();
                removed = true;
            } else if (record.getValue() != null) {
                removed = true;
            } else if (record.lsValues != null) {
                removed = true;
            }

            if (removed) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record);
                record.setVersion(record.getVersion() + 1);
                if (record.getValue() != null) {
                    record.getValue().setNoData();
                    record.setValue(null);
                } else if (record.lsValues != null) {
                    for (Data v : record.lsValues) {
                        v.setNoData();
                    }
                    record.lsValues = null;
                }
            }

            req.version = record.getVersion();
            req.longValue = record.copyCount;

            if (record.isRemovable()) {
                markAsRemoved(record);
            }
            return true;
        }

        public Data remove(Request req) {
            Record record = mapRecords.get(req.key);
            if (record == null) {
                return null;
            }
            if (req.txnId != -1) {
                unlock(record);
            }
            if (!record.isValid()) {
                if (record.isEvictable()) {
                    scheduleForEviction(record);
                    return null;
                }
            }
            if (req.value != null) {
                if (record.getValue() != null) {
                    if (!record.getValue().equals(req.value)) {
                        return null;
                    }
                }
            }
            Data oldValue = record.getValue();
            if (oldValue == null && record.lsValues != null && record.lsValues.size() > 0) {
                Values values = new Values(record.lsValues);
                oldValue = toData(values);
                record.lsValues = null;
            }
            if (oldValue != null) {
                removeValueIndex(oldValue, record);
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record.getKey(), oldValue, record.mapListeners);
                record.incrementVersion();
                record.setValue(null);
                record.lsValues = null;
            }
            req.version = record.getVersion();
            req.longValue = record.copyCount;

            if (record.isRemovable()) {
                markAsRemoved(record);
            }
            if (oldValue != null) {
                req.key.setNoData();
                req.key = null;
            }
            return oldValue;
        }

        void reset() {
            if (locallyOwnedMap != null) {
                locallyOwnedMap.reset();
            }
            mapRecords.clear();
            ownedEntryCount = 0;
            evicting = false;
            if (setDirtyRecords != null) {
                setDirtyRecords.clear();
            }
            setRemovedRecords.clear();
        }

        void scheduleForEviction(Record record) {
            SortedHashMap.moveToTop(mapRecords, record.getKey());
        }

        void touch(Record record) {
            record.lastTouchTime = System.currentTimeMillis();
            if (evictionPolicy == OrderingType.NONE) return;
            SortedHashMap.touch(mapRecords, record.getKey(), evictionPolicy);
        }

        void markAsDirty(Record record) {
            if (!record.dirty) {
                record.dirty = true;
                if (writeDelaySeconds > 0) {
                    record.writeTime = System.currentTimeMillis() + (writeDelaySeconds * 1000L);
                    setDirtyRecords.add(record);
                }
            }
        }

        void markAsActive(Record record) {
            if (!record.isActive()) {
                record.setActive();
                setRemovedRecords.remove(record);
            }
        }

        boolean shouldRemove(Record record, long now) {
            return !record.isActive() && ((now - record.removeTime) > removeDelayMillis);
        }

        void markAsRemoved(Record record) {
            if (record.isActive()) {
                record.markRemoved();
                setRemovedRecords.add(record);
                Block ownerBlock = blocks[record.blockId];
                if (thisAddress.equals(ownerBlock.getRealOwner())) {
                    ownedEntryCount--;
                }
            }
        }

        void removeAndPurgeRecord(Record record) {
            Record removedRecord = mapRecords.remove(record.getKey());
            if (removedRecord != record) {
                throw new RuntimeException(removedRecord + " is removed but should have removed " + record);
            }
            garbage(record.getKey());
            if (record.getValue() != null) {
                garbage(record.getValue());
            }
            if (record.lsValues != null) {
                for (Data data : record.lsValues) {
                    garbage(data);
                }
            }
        }


        void addNewValueIndex(Record record) {
            Integer hash = record.getValue().hashCode();
            Set<Record> lsRecords = mapValueIndex.get(hash);
            if (lsRecords == null) {
                lsRecords = new LinkedHashSet<Record>();
                mapValueIndex.put(hash, lsRecords);
            }
            lsRecords.add(record);
        }

        void updateValueIndex(Data oldValue, Record record) {
            if (oldValue == null || oldValue.size() == 0) {
                addNewValueIndex(record);
            } else {
                if (oldValue.hashCode() != record.getValue().hashCode()) {
                    removeValueIndex(oldValue, record);
                    addNewValueIndex(record);
                }
            }
        }

        void removeValueIndex(Data oldValue, Record record) {
            Integer hash = oldValue.hashCode();
            Set<Record> lsRecords = mapValueIndex.get(hash);
            if (lsRecords != null && lsRecords.size() > 0) {
                lsRecords.remove(record);
                if (lsRecords.size() == 0) {
                    mapValueIndex.remove(hash);
                }
            }
        }

        boolean searchValueIndex(Data value) {
            Set<Record> lsRecords = mapValueIndex.get(value.hashCode());
            if (lsRecords == null || lsRecords.size() == 0) {
                return false;
            } else {
                for (Record rec : lsRecords) {
                    if (rec.isValid()) {
                        if (value.equals(rec.getValue())) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        Record createNewRecord(Data key, Data value) {
            if (key == null || key.size() == 0) {
                throw new RuntimeException("Cannot create record from a 0 size key: " + key);
            }
            int blockId = getBlockId(key);
            Record rec = new Record(node.factory, name, blockId, key, value, ttl, newId++);
            Block ownerBlock = getOrCreateBlock(blockId);
            if (thisAddress.equals(ownerBlock.getRealOwner())) {
                if (value != null && value.size() > 0) {
                    addNewValueIndex(rec);
                }
                ownedEntryCount++;
            }
            mapRecords.put(key, rec);
            if (evictionPolicy != OrderingType.NONE) {
                if (maxSize != Integer.MAX_VALUE) {
                    int limitSize = (maxSize / lsMembers.size());
                    if (ownedEntryCount > limitSize) {
                        startEviction();
                    }
                }
            }
            return rec;
        }

        public void addListener(Data key, Address address, boolean includeValue) {
            if (key == null || key.size() == 0) {
                mapListeners.put(address, includeValue);
            } else {
                Record rec = getRecord(key);
                if (rec == null) {
                    rec = createNewRecord(key, null);
                }
                rec.addListener(address, includeValue);
            }
        }

        public void removeListener(Data key, Address address) {
            if (key == null || key.size() == 0) {
                mapListeners.remove(address);
            } else {
                Record rec = getRecord(key);
                if (rec != null) {
                    rec.removeListener(address);
                }
            }
        }

        @Override
        public String toString() {
            return "CMap [" + name + "] size=" + size() + ", backup-size=" + backupSize();
        }
    }


    class VersionedBackupOp implements Runnable, Comparable {
        final Request request;

        VersionedBackupOp(Request request) {
            this.request = request;
        }

        public void run() {
            CMap cmap = getMap(request.name);
            cmap.doBackup(request);
        }

        long getVersion() {
            return request.version;
        }

        public int compareTo(Object o) {
            long v = ((VersionedBackupOp) o).getVersion();
            if (request.version > v) return 1;
            else if (request.version < v) return -1;
            else return 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            VersionedBackupOp that = (VersionedBackupOp) o;

            return request.version == that.request.version;

        }

        @Override
        public int hashCode() {
            return (int) (request.version ^ (request.version >>> 32));
        }
    }

    public void fireScheduledActions(Record record) {
        if (record.lockCount == 0) {
            record.lockThreadId = -1;
            record.lockAddress = null;
            if (record.lsScheduledActions != null) {
                while (record.lsScheduledActions.size() > 0) {
                    ScheduledAction sa = record.lsScheduledActions.remove(0);
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
        List<ScheduledAction> lsScheduledActions = record.lsScheduledActions;
        if (lsScheduledActions != null) {
            if (lsScheduledActions.size() > 0) {
                Iterator<ScheduledAction> it = lsScheduledActions.iterator();
                while (it.hasNext()) {
                    ScheduledAction sa = it.next();
                    if (sa.request.caller.equals(deadAddress)) {
                        node.clusterManager.deregisterScheduledAction(sa);
                        sa.setValid(false);
                        it.remove();
                    }
                }
            }
        }
        if (record.lockCount > 0) {
            if (deadAddress.equals(record.lockAddress)) {
                unlock(record);
            }
        }
    }

    public void unlock(Record record) {
        record.lockThreadId = -1;
        record.lockCount = 0;
        record.lockAddress = null;
        fireScheduledActions(record);
    }

    public static class Record {
        private static final Logger logger = Logger.getLogger(Record.class.getName());

        private final AtomicReference<Data> key = new AtomicReference<Data>();
        private final AtomicReference<Data> value = new AtomicReference<Data>();
        private final AtomicLong version = new AtomicLong();
        private final AtomicInteger hits = new AtomicInteger(0);
        private final AtomicBoolean active = new AtomicBoolean(true);
        private final AtomicLong lastAccessTime = new AtomicLong(0);
        private final long creationTime;
        private long lastTouchTime = 0;
        private long expirationTime = Long.MAX_VALUE;
        private long lastUpdateTime = 0;
        private final FactoryImpl factory;
        private final String name;
        private final int blockId;
        private int lockThreadId = -1;
        private Address lockAddress = null;
        private int lockCount = 0;
        private List<ScheduledAction> lsScheduledActions = null;
        private Map<Address, Boolean> mapListeners = null;
        private int copyCount = 0;
        private List<Data> lsValues = null; // multimap values
        private SortedSet<VersionedBackupOp> backupOps = null;
        private boolean dirty = false;
        private long writeTime = -1;
        private long removeTime;

        private final RecordEntry recordEntry;
        private final long id;
        private final long[] oldIndexes = new long[6];

        public Record(FactoryImpl factory, String name, int blockId, Data key, Data value, long ttl, long id) {
            super();
            this.factory = factory;
            this.name = name;
            this.blockId = blockId;
            this.setKey(key);
            this.setValue(value);
            this.creationTime = System.currentTimeMillis();
            setExpirationTime(ttl);
            this.lastTouchTime = getCreationTime();
            this.setVersion(0);
            recordEntry = new RecordEntry(this);
            this.id = id;
            int len = oldIndexes.length;
            for (int i = 0; i < len; i++) {
                oldIndexes[i] = Long.MIN_VALUE;
            }
        }

        public RecordEntry getRecordEntry() {
            return recordEntry;
        }

        public void runBackupOps() {
            if (backupOps != null) {
                if (backupOps.size() > 0) {
                    Iterator<VersionedBackupOp> it = backupOps.iterator();
                    while (it.hasNext()) {
                        VersionedBackupOp bo = it.next();
                        if (bo.getVersion() < getVersion() + 1) {
                            it.remove();
                        } else if (bo.getVersion() == getVersion() + 1) {
                            bo.run();
                            setVersion(bo.getVersion());
                            bo.request.reset();
                            it.remove();
                        } else {
                            return;
                        }
                    }
                }
            }
        }

        public void addBackupOp(VersionedBackupOp bo) {
            if (backupOps == null) {
                backupOps = new TreeSet<VersionedBackupOp>();
            }
            backupOps.add(bo);
            if (backupOps.size() > 4) {
                logger.log(Level.FINEST, " Forcing backup.run version " + getVersion());
                forceBackupOps();
            }
        }

        public void forceBackupOps() {
            if (backupOps == null) return;
            Iterator<VersionedBackupOp> it = backupOps.iterator();
            while (it.hasNext()) {
                VersionedBackupOp v = it.next();
                v.run();
                setVersion(v.getVersion());
                v.request.reset();
                it.remove();
            }
        }

        public Data getKey() {
            return key.get();
        }

        public void setKey(Data key) {
            this.key.set(key);
        }

        public Data getValue() {
            return value.get();
        }

        public void setValue(Data value) {
            this.value.set(value);
        }

        public int valueCount() {
            int count = 0;
            if (getValue() != null) {
                count = 1;
            } else if (lsValues != null) {
                count = lsValues.size();
            } else if (copyCount > 0) {
                count += copyCount;
            }
            return count;
        }

        public long getCost() {
            long cost = 0;
            if (getValue() != null) {
                cost = getValue().size();
                if (copyCount > 0) {
                    cost *= copyCount;
                }
            } else if (lsValues != null) {
                for (Data data : lsValues) {
                    cost += data.size();
                }
            }
            return cost + getKey().size();
        }

        public boolean containsValue(Data value) {
            if (this.getValue() != null) {
                return this.getValue().equals(value);
            } else if (lsValues != null) {
                int count = lsValues.size();
                for (int i = 0; i < count; i++) {
                    if (value.equals(lsValues.get(i))) {
                        return true;
                    }
                }
            }
            return false;
        }

        public void addValue(Data value) {
            if (lsValues == null) {
                lsValues = new ArrayList<Data>(2);
            }
            lsValues.add(value);
        }


        public boolean unlock(int threadId, Address address) {
            if (threadId == -1 || address == null)
                throw new IllegalArgumentException();
            if (lockCount == 0)
                return true;
            if (lockThreadId != threadId || !address.equals(lockAddress)) {
                return false;
            }
            if (lockCount > 0) {
                lockCount--;
            }

            return true;
        }

        public boolean testLock(int threadId, Address address) {
            return lockCount == 0 || lockThreadId == threadId && lockAddress.equals(address);
        }

        public boolean lock(int threadId, Address address) {
            if (lockCount == 0) {
                lockThreadId = threadId;
                lockAddress = address;
                lockCount++;
                return true;
            }
            if (lockThreadId == threadId && lockAddress.equals(address)) {
                lockCount++;
                return true;
            }
            return false;
        }

        public void addScheduledAction(ScheduledAction scheduledAction) {
            if (lsScheduledActions == null) {
                lsScheduledActions = new ArrayList<ScheduledAction>(1);
            }
            lsScheduledActions.add(scheduledAction);
            logger.log(Level.FINEST, scheduledAction.request.operation + " scheduling " + scheduledAction);
        }

        public boolean isRemovable() {
            return (valueCount() <= 0 && !hasListener() && (lsScheduledActions == null || lsScheduledActions.size() == 0) && (backupOps == null || backupOps.size() == 0));
        }

        public boolean isEvictable() {
            return (lockCount == 0 && !hasListener() && (lsScheduledActions == null || lsScheduledActions.size() == 0));
        }

        public boolean hasListener() {
            return (mapListeners != null && mapListeners.size() > 0);
        }

        public void addListener(Address address, boolean returnValue) {
            if (mapListeners == null)
                mapListeners = new HashMap<Address, Boolean>(1);
            mapListeners.put(address, returnValue);
        }

        public void removeListener(Address address) {
            if (mapListeners == null)
                return;
            mapListeners.remove(address);
        }

        public void incrementCopyCount() {
            ++copyCount;
        }

        public void decrementCopyCount() {
            if (copyCount > 0) {
                --copyCount;
            }
        }

        public int getCopyCount() {
            return copyCount;
        }

        public void setLastUpdated() {
            setLastUpdateTime(System.currentTimeMillis());
        }

        public void setLastAccessed() {
            setLastAccessTime(System.currentTimeMillis());
            incrementHits();
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public void setExpirationTime(long ttl) {
            if (ttl == 0) {
                expirationTime = Long.MAX_VALUE;
            } else {
                expirationTime = getCreationTime() + ttl;
            }
        }

        public boolean isValid(long now) {
            return expirationTime > now;
        }

        public boolean isValid() {
            return expirationTime > System.currentTimeMillis();
        }

        public void markRemoved() {
            setActive(false);
            removeTime = System.currentTimeMillis();
            int len = oldIndexes.length;
            for (int i = 0; i < len; i++) {
                oldIndexes[i] = Long.MIN_VALUE;
            }
        }

        public void setActive() {
            setActive(true);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Record record = (Record) o;
            return (record.id == id);
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }

        public String toString() {
            return "Record key=" + getKey() + ", removable=" + isRemovable();
        }


        public long getVersion() {
            return version.get();
        }

        public void setVersion(long version) {
            this.version.set(version);
        }

        public void incrementVersion() {
            this.version.incrementAndGet();
        }

        public long getCreationTime() {
            return creationTime;
        }

        public long getLastAccessTime() {
            return lastAccessTime.get();
        }

        public void setLastAccessTime(long lastAccessTime) {
            this.lastAccessTime.set(lastAccessTime);
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public void setLastUpdateTime(long lastUpdateTime) {
            this.lastUpdateTime = lastUpdateTime;
        }

        public int getHits() {
            return hits.get();
        }

        public void incrementHits() {
            this.hits.incrementAndGet();
        }

        public boolean isActive() {
            return active.get();
        }

        public void setActive(boolean active) {
            this.active.set(active);
        }

        public long getAndSetIndex(int i, long newValue) {
            long oldValue = oldIndexes[i];
            oldIndexes[i] = newValue;
            return oldValue;
        }


        static class RecordEntry implements MapEntry {

            private final Record record;

            RecordEntry(Record record) {
                this.record = record;
            }

            public boolean isValid() {
                return record.isActive();
            }

            public Object getKey() {
                return toObject(record.getKey(), false);
            }

            public Object getValue() {
                return toObject(record.getValue(), false);
            }

            public Object setValue(Object value) {
                FactoryImpl.MProxy proxy = (FactoryImpl.MProxy) record.factory.getOrCreateProxyByName(record.name);
                return proxy.put(getKey(), value);
            }

            public long getCost() {
                return record.getCost();
            }

            public long getExpirationTime() {
                return record.getExpirationTime();
            }

            public long getVersion() {
                return record.getVersion();
            }

            public long getCreationTime() {
                return record.getCreationTime();
            }

            public long getLastAccessTime() {
                return record.getLastAccessTime();
            }

            public long getLastUpdateTime() {
                return record.getLastUpdateTime();
            }

            public int getHits() {
                return record.getHits();
            }
        }
    }

    public static class Block implements DataSerializable {
        int blockId;
        Address owner;
        Address migrationAddress;

        public Block() {
        }

        public Block(int blockId, Address owner) {
            this.blockId = blockId;
            this.owner = owner;
        }

        public boolean isMigrating() {
            return (migrationAddress != null);
        }

        public Address getRealOwner() {
            return (migrationAddress != null) ? migrationAddress : owner;
        }

        public void readData(DataInput in) throws IOException {
            blockId = in.readInt();
            boolean owned = in.readBoolean();
            if (owned) {
                owner = new Address();
                owner.readData(in);
            }
            boolean migrating = in.readBoolean();
            if (migrating) {
                migrationAddress = new Address();
                migrationAddress.readData(in);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(blockId);
            boolean owned = (owner != null);
            out.writeBoolean(owned);
            if (owned) {
                owner.writeData(out);
            }
            boolean migrating = (migrationAddress != null);
            out.writeBoolean(migrating);
            if (migrating)
                migrationAddress.writeData(out);
        }

        @Override
        public String toString() {
            return "Block [" + blockId + "] owner=" + owner + " migrationAddress="
                    + migrationAddress;
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

        public Entries(String name, ClusterOperation operation) {
            this.name = name;
            this.operation = operation;
            TransactionImpl txn = ThreadContext.get().txn;
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
            TransactionImpl txn = ThreadContext.get().txn;
            for (KeyValue entry : pairs.lsKeyValues) {
                if (txn != null) {
                    Object key = entry.getKey();
                    if (txn.has(name, key)) {
                        Object value = txn.get(name, key);
                        if (value != null) {
                            lsKeyValues.add(createSimpleEntry(node.factory, name, key, value));
                        }
                    } else {
                        entry.setName(node.factory.getName(), name);
                        lsKeyValues.add(entry);
                    }
                } else {
                    entry.setName(node.factory.getName(), name);
                    lsKeyValues.add(entry);
                }
            }
        }

        class EntryIterator implements Iterator {
            final Iterator<Map.Entry> it;
            Map.Entry entry = null;

            public EntryIterator(Iterator<Map.Entry> it) {
                super();
                this.it = it;
            }

            public boolean hasNext() {
                return it.hasNext();
            }

            public Object next() {
                entry = it.next();
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
            throw new UnsupportedOperationException();
        }

        public Object[] toArray(Object[] a) {
            throw new UnsupportedOperationException();
        }

    }


    public static class CMapEntry implements MapEntry, DataSerializable {
        private long cost = 0;
        private long expirationTime = 0;
        private long lastAccessTime = 0;
        private long lastUpdateTime = 0;
        private long creationTime = 0;
        private long version = 0;
        private int hits = 0;
        private boolean valid = true;
        private String factoryName = null;
        private String name = null;
        private Object key = null;
        private Object value = null;

        public CMapEntry() {
        }

        public CMapEntry(long cost, long expirationTime, long lastAccessTime, long lastUpdateTime, long creationTime, long version, int hits, boolean valid) {
            this.cost = cost;
            this.expirationTime = expirationTime;
            this.lastAccessTime = lastAccessTime;
            this.lastUpdateTime = lastUpdateTime;
            this.creationTime = creationTime;
            this.version = version;
            this.hits = hits;
            this.valid = valid;
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeLong(cost);
            out.writeLong(expirationTime);
            out.writeLong(lastAccessTime);
            out.writeLong(lastUpdateTime);
            out.writeLong(creationTime);
            out.writeLong(version);
            out.writeInt(hits);
            out.writeBoolean(valid);
        }

        public void readData(DataInput in) throws IOException {
            cost = in.readLong();
            expirationTime = in.readLong();
            lastAccessTime = in.readLong();
            lastUpdateTime = in.readLong();
            creationTime = in.readLong();
            version = in.readLong();
            hits = in.readInt();
            valid = in.readBoolean();
        }

        public void set(String factoryName, String name, Object key) {
            this.factoryName = factoryName;
            this.name = name;
            this.key = key;
        }

        public long getCost() {
            return cost;
        }

        public long getCreationTime() {
            return creationTime;
        }

        public long getExpirationTime() {
            return expirationTime;
        }

        public long getLastUpdateTime() {
            return lastUpdateTime;
        }

        public int getHits() {
            return hits;
        }

        public long getLastAccessTime() {
            return lastAccessTime;
        }

        public long getVersion() {
            return version;
        }

        public boolean isValid() {
            return valid;
        }

        public Object getKey() {
            return key;
        }

        public Object getValue() {
            if (value == null) {
                FactoryImpl factory = FactoryImpl.getFactoryImpl(factoryName);
                value = ((FactoryImpl.MProxy) factory.getOrCreateProxyByName(name)).get(key);
            }
            return value;
        }

        public Object setValue(Object value) {
            Object oldValue = this.value;
            FactoryImpl factory = FactoryImpl.getFactoryImpl(factoryName);
            ((FactoryImpl.MProxy) factory.getOrCreateProxyByName(name)).put(key, value);
            return oldValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            CMapEntry cMapEntry = (CMapEntry) o;

            return !(key != null ? !key.equals(cMapEntry.key) : cMapEntry.key != null) &&
                    !(name != null ? !name.equals(cMapEntry.name) : cMapEntry.name != null);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + (key != null ? key.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("MapEntry");
            sb.append("{key=").append(key);
            sb.append(", valid=").append(valid);
            sb.append(", hits=").append(hits);
            sb.append(", version=").append(version);
            sb.append(", creationTime=").append(creationTime);
            sb.append(", lastUpdateTime=").append(lastUpdateTime);
            sb.append(", lastAccessTime=").append(lastAccessTime);
            sb.append(", expirationTime=").append(expirationTime);
            sb.append(", cost=").append(cost);
            sb.append('}');
            return sb.toString();
        }
    }


    public static class Values implements Collection, DataSerializable {
        List<Data> lsValues = null;

        public Values() {
        }

        public Values(List<Data> lsValues) {
            super();
            this.lsValues = lsValues;
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
            throw new UnsupportedOperationException();
        }

        public boolean containsAll(Collection c) {
            throw new UnsupportedOperationException();
        }

        public boolean isEmpty() {
            return (size() == 0);
        }

        public Iterator iterator() {
            return new ValueIterator(lsValues.iterator());
        }

        class ValueIterator implements Iterator {
            final Iterator<Data> it;

            public ValueIterator(Iterator<Data> it) {
                super();
                this.it = it;
            }

            public boolean hasNext() {
                return it.hasNext();
            }

            public Object next() {
                Data value = it.next();
                return ThreadContext.get().toObject(value);
            }

            public void remove() {
                it.remove();
            }
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

        public int size() {
            return lsValues.size();
        }

        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        public Object[] toArray(Object[] a) {
            throw new UnsupportedOperationException();
        }

        public void readData(DataInput in) throws IOException {
            int size = in.readInt();
            lsValues = new ArrayList<Data>(size);
            for (int i = 0; i < size; i++) {
                Data data = createNewData();
                data.readData(in);
                lsValues.add(data);
            }
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeInt(lsValues.size());
            for (Data data : lsValues) {
                data.writeData(out);
            }
        }
    }
}
