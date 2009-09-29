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
import com.hazelcast.nio.Address;
import static com.hazelcast.nio.BufferUtil.*;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.nio.Packet;
import com.hazelcast.query.Expression;
import com.hazelcast.query.Index;
import com.hazelcast.query.Predicate;

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
    private static final int BLOCK_COUNT = ConfigProperty.CONCURRENT_MAP_BLOCK_COUNT.getInteger(271);
    private final Block[] blocks;
    private final ConcurrentMap<String, CMap> maps;
    private final ConcurrentMap<String, LocallyOwnedMap> mapLocallyOwnedMaps;
    private final OrderedExecutionTask[] orderedExecutionTasks;
    private static long GLOBAL_REMOVE_DELAY_MILLIS = ConfigProperty.REMOVE_DELAY_SECONDS.getLong() * 1000L;
    private long newRecordId = 0;

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
    }

    public void reset() {
        maps.clear();
    }

    public void syncForDead(Address deadAddress) {
        if (deadAddress.equals(thisAddress)) return;
        for (Block block : blocks) {
            if (block != null) {
                if (deadAddress.equals(block.getOwner())) {
                    MemberImpl member = getNextMemberBeforeSync(block.getOwner(), true, 1);
                    if (member == null) {
                        if (!isSuperClient()) {
                            block.setOwner(thisAddress);
                        } else {
                            block.setOwner(null);
                        }
                    } else {
                        if (!deadAddress.equals(member.getAddress())) {
                            block.setOwner(member.getAddress());
                        } else {
                            block.setOwner(null);
                        }
                    }
                }
                if (block.getMigrationAddress() != null) {
                    if (deadAddress.equals(block.getMigrationAddress())) {
                        MemberImpl member = getNextMemberBeforeSync(block.getMigrationAddress(), true, 1);
                        if (member == null) {
                            if (!isSuperClient()) {
                                block.setMigrationAddress(thisAddress);
                            } else {
                                block.setMigrationAddress(null);
                            }
                        } else {
                            if (!deadAddress.equals(member.getAddress())) {
                                block.setMigrationAddress(member.getAddress());
                            } else {
                                block.setMigrationAddress(null);
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
                if (block.getOwner() == null) {
                    lsBlocksToRedistribute.add(block);
                } else {
                    if (!block.isMigrating()) {
                        Integer countInt = addressBlocks.get(block.getOwner());
                        int count = (countInt == null) ? 0 : countInt;
                        if (count >= aveBlockOwnCount) {
                            lsBlocksToRedistribute.add(block);
                        } else {
                            count++;
                            addressBlocks.put(block.getOwner(), count);
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
                        if (blockToMigrate.getOwner() == null) {
                            blockToMigrate.setOwner(address);
                        } else {
                            blockToMigrate.setMigrationAddress(address);
                            if (blockToMigrate.getOwner().equals(blockToMigrate.getMigrationAddress())) {
                                blockToMigrate.setMigrationAddress(null);
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
                if (block.getOwner() == null) {
                    int index = addressIndex++ % addressBlocks.size();
                    block.setOwner((Address) addressBlocks.keySet().toArray()[index]);
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
                        cmap.addIndex(mapIndex.expression, mapIndex.ordered);
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
        Expression expression;
        boolean ordered;

        public AddMapIndex() {
        }

        public AddMapIndex(String mapName, Expression expression, boolean ordered) {
            this.mapName = mapName;
            this.expression = expression;
            this.ordered = ordered;
        }

        public void process() {
            CMap cmap = getNode().concurrentMapManager.getOrCreateMap(mapName);
            cmap.addIndex(expression, ordered);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(mapName);
            out.writeBoolean(ordered);
            writeObject(out, expression);
        }

        public void readData(DataInput in) throws IOException {
            mapName = in.readUTF();
            ordered = in.readBoolean();
            expression = (Expression) readObject(in);
        }
    }

    volatile boolean migrating = false;

    void doResetRecords() {
        if (!node.active || node.factory.restarted) return;
        if (migrating) {
            throw new RuntimeException("Migration is already in progress");
        }
        InitialState initialState = new InitialState();
        Collection<CMap> cmaps = maps.values();
        for (final CMap cmap : cmaps) {
            initialState.createAndAddMapState(cmap);
        }
        sendProcessableToAll(initialState, false);
        if (isSuperClient())
            return;
        migrating = true;
        cmaps = maps.values();
        final AtomicInteger count = new AtomicInteger(0);
        for (final CMap cmap : cmaps) {
            final Object[] records = cmap.mapRecords.values().toArray();
            cmap.reset();
            for (Object recObj : records) {
                final Record rec = (Record) recObj;
                if (rec.isActive()) {
                    if (rec.getKey() == null || rec.getKey().size() == 0) {
                        throw new RuntimeException("Record.key is null or empty " + rec.getKey());
                    }
                    count.incrementAndGet();
                    executeLocally(new FallThroughRunnable() {
                        public void doRun() {
                            try {
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
                            } finally {
                                count.decrementAndGet();
                            }
                        }
                    });
                }
            }
        }
        executeLocally(new FallThroughRunnable() {
            public void doRun() {
                while (count.get() != 0 && node.active) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                        return;
                    }
                }
                MultiMigrationComplete mmc = new MultiMigrationComplete();
                mmc.call();
                logger.log(Level.FINEST, "Migration ended!");
                migrating = false;
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

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MEvict extends MBackupAndMigrationAwareOp {
        public boolean evict(String name, Object key) {
            Data k = (key instanceof Data) ? (Data) key : toData(key);
            request.setLocal(CONCURRENT_MAP_EVICT, name, k, null, 0, -1, thisAddress);
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
                    target = block.getMigrationAddress();
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
            TransactionImpl txn = tc.callContext.txn;
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

        @Override
        public boolean isMigrationAware() {
            return true;
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
            TransactionImpl txn = threadContext.callContext.txn;
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
            TransactionImpl txn = threadContext.callContext.txn;
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
            Data key = ThreadContext.get().toData(value);
            boolean result = booleanCall(CONCURRENT_MAP_ADD_TO_LIST, name, key, null, 0, -1);
            backup(CONCURRENT_MAP_BACKUP_ADD);
            return result;
        }

        boolean addToSet(String name, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.callContext.txn;
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
                        newIndexes[i] = index.extractLongValue(value);
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
            TransactionImpl txn = threadContext.callContext.txn;
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
                            oldObject = toObject(oldValue);
                        }
                        txn.attachPutOp(name, key, value, (oldObject == null));
                        return threadContext.isClient() ? oldValue : oldObject;
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

    class MRemoveMulti extends MBackupAndMigrationAwareOp {

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

        class MGetContainsValue extends MigrationAwareTargettedCall {
            public MGetContainsValue(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_CONTAINS_VALUE, name, null, value, 0, -1);
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
            TransactionImpl txn = ThreadContext.get().callContext.txn;
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

        class MGetEntries extends MigrationAwareTargettedCall {
            public MGetEntries(Address target) {
                this.target = target;
                request.reset();
                setLocal(operation, name, null, predicate, -1, -1);
            }
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
        if (migrating && req.key != null) {
            CMap cmap = getMap(req.name);
            if (cmap != null && cmap.getRecord(req.key) != null) {
                return false;
            }
        }
        return migrating;
    }

    private int getBlockId(Data key) {
        int hash = key.hashCode();
        return Math.abs(hash) % BLOCK_COUNT;
    }

    Block getOrCreateBlock(Data key) {
        return getOrCreateBlock(getBlockId(key));
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

    LocallyOwnedMap getLocallyOwnedMap(String name) {
        return mapLocallyOwnedMaps.get(name);
    }

    CMap getMap(String name) {
        return maps.get(name);
    }

    CMap getOrCreateMap(String name) {
        checkServiceThread();
        CMap map = maps.get(name);
        if (map == null) {
            map = new CMap(name);
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
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.removeItem(request);
        }
    }

    class RemoveOperationHandler extends StoreAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.remove(request);
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
            request.response = cmap.put(request);
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
                request.value = doHardCopy(rec.getValue());
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
                AtomicBoolean strongRef = new AtomicBoolean(false);
                Set<MapEntry> results = node.queryService.query(cmap.name, strongRef, predicate);
//                System.out.println(node.getName() + " after index REcords size " + results.size() + " strong: " + strongRef.get());
                if (predicate != null) {
                    if (!strongRef.get()) {
                        Iterator<MapEntry> it = results.iterator();
                        while (it.hasNext()) {
                            Record record = (Record) it.next();
                            if (record.isActive()) {
                                try {
                                    if (!predicate.apply(record.getRecordEntry())) {
                                        it.remove();
                                    } else {
//                                      System.out.println(node.getName() + " FOUND " + record.getValue());
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

    class LocallyOwnedMap {
        final ConcurrentMap<Object, Record> mapCache = new ConcurrentHashMap<Object, Record>(1000);
        final private Queue<Record> localRecords = new ConcurrentLinkedQueue<Record>();

        public Object get(Object key) {
            processLocalRecords();
            Record record = mapCache.get(key);
            if (record == null) {
                return OBJECT_REDO;
            } else {
                if (record.isActive()) {
                    try {
                        Object value = toObject(record.getValue());
                        record.setLastAccessed();
                        return value;
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
            Object key = toObject(record.getKey());
            mapCache.put(key, record);
        }

        public void offerToCache(Record record) {
            localRecords.offer(record);
        }
    }

    public class CMap {
        final Set<Record> setRemovedRecords = new HashSet<Record>(10000);

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

        int ownedEntryCount = 0;

        private Set<Record> setDirtyRecords = null;

        private final long removeDelayMillis;

        private final Map<Expression, Index<MapEntry>> mapIndexes = new ConcurrentHashMap(10);

        private volatile Index<MapEntry>[] indexes = null;

        private volatile byte[] indexTypes = null;

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
                            setDirtyRecords = new HashSet<Record>(5000);
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

        public void addIndex(Expression expression, boolean ordered) {
            if (!mapIndexes.containsKey(expression)) {
                Index index = new Index(expression, ordered);
                mapIndexes.put(expression, index);
                Index[] newIndexes = new Index[mapIndexes.size()];
                if (indexes != null) {
                    System.arraycopy(indexes, 0, newIndexes, 0, indexes.length);
                }
                indexes = newIndexes;
                indexes[mapIndexes.size() - 1] = index;
                node.queryService.setIndexes(name, indexes, mapIndexes);
            }
        }

        public Record getRecord(Data key) {
            return mapRecords.get(key);
        }

        public int getBackupCount() {
            return backupCount;
        }

        public void own(Request req) {
            if (req.key == null || req.key.size() == 0) {
                throw new RuntimeException("Key cannot be null " + req.key);
            }
            if (req.value == null) {
                req.value = new Data();
            }
            Record record = getRecord(req.key);
            boolean created = false;
            if (record == null) {
                record = toRecord(req);
                created = true;
            }
            updateIndexes(created, req, record);
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
                    record.addBackupOp(new VersionedBackupOp(CMap.this, reqCopy));
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
                if (req.indexes != null) {
                    if (req.indexTypes == null) {
                        throw new RuntimeException("index types cannot be null!");
                    }
                    if (req.indexes.length != req.indexTypes.length) {
                        throw new RuntimeException("index and type lenghts do not match");
                    }
                    rec.setIndexes(req.indexes);
                    if (indexTypes == null) {
                        indexTypes = req.indexTypes;
                    } else {
                        if (indexTypes.length != req.indexTypes.length) {
                            throw new RuntimeException("Index types do not match.");
                        }
                    }
                }
            } else if (req.operation == CONCURRENT_MAP_BACKUP_REMOVE) {
                Record record = getRecord(req.key);
                if (record != null) {
                    if (record.getCopyCount() > 0) {
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
                            if (record.getMultiValues() != null) {
                                Iterator<Data> itValues = record.getMultiValues().iterator();
                                while (itValues.hasNext()) {
                                    Data value = itValues.next();
                                    if (req.value.equals(value)) {
                                        itValues.remove();
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
                Block block = blocks[record.getBlockId()];
                if (!thisAddress.equals(block.getOwner())) {
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
                if (record.isActive() && record.isValid(now)) {
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
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
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
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
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
                        touch(record);
                        if (value == null) {
                            return record.valueCount() > 0;
                        } else {
                            return record.containsValue(value);
                        }
                    }
                }
            } else {
                Collection<Record> records = mapRecords.values();
                for (Record record : records) {
                    Block block = blocks[record.getBlockId()];
                    if (thisAddress.equals(block.getOwner())) {
                        if (record.containsValue(value)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }

        public CMapEntry getMapEntry(Request req) {
            Record record = getRecord(req.key);
            if (record == null)
                return null;
            if (!record.isValid()) {
                return null;
            }
            return new CMapEntry(record.getCost(), record.getExpirationTime(), record.getLastAccessTime(), record.getLastUpdateTime(),
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
                returnValue = data;
            } else {
                if (record.getMultiValues() != null) {
                    Values values = new Values(record.getMultiValues());
                    returnValue = toData(values);
                }
            }
            if (returnValue != null) {
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
            node.queryService.updateIndex(name, null, null, record, Integer.MIN_VALUE);
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
                    if (record.getMultiValues() != null) {
                        Iterator<Data> itValues = record.getMultiValues().iterator();
                        while (itValues.hasNext()) {
                            Data value = itValues.next();
                            if (req.value.equals(value)) {
                                itValues.remove();
                                removed = true;
                            }
                        }
                    }
                }
            }
            if (removed) {
                record.setVersion(record.getVersion() + 1);
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record.getKey(), req.value, record.getMapListeners());
                logger.log(Level.FINEST, record.getValue() + " RemoveMulti " + record.getMultiValues());
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
                if (!record.isActive()) {
                    markAsActive(record);
                    ownedEntryCount++;
                }
                if (record.containsValue(req.value)) {
                    added = false;
                }
            }
            if (added) {
                node.queryService.updateIndex(name, null, null, record, Integer.MIN_VALUE);
                record.addValue(req.value);
                req.value = null;
                record.setVersion(record.getVersion() + 1);
                touch(record);
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record.getKey(), req.value, record.getMapListeners());
            }
            logger.log(Level.FINEST, record.getValue() + " PutMulti " + record.getMultiValues());
            req.version = record.getVersion();
            return added;
        }

        public Data put(Request req) {
            if (ownedEntryCount >= maxSize) {
                startEviction();
            }
            if (req.value == null) {
                req.value = new Data();
            }
            if (req.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                Record record = recordExist(req);
                if (record != null && record.isActive() && record.getValue() != null) {
                    return record.getValue();
                }
            } else if (req.operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL) {
                Record record = recordExist(req);
                if (record == null || !record.isActive() || record.getValue() == null) {
                    return null;
                }
            }
            Record record = getRecord(req.key);
            Data oldValue = null;
            boolean created = true;
            if (record == null) {
                record = createNewRecord(req.key, req.value);
                req.key = null;
            } else {
                created = !record.isActive();
                if (created) {
                    ownedEntryCount++;
                }
                markAsActive(record);
                if (!record.isValid()) {
                    record.setExpirationTime(ttl);
                }
                oldValue = record.getValue();
                record.setValue(req.value);
                record.incrementVersion();
                touch(record);
                record.setLastUpdated();
            }
            req.version = record.getVersion();
            req.longValue = record.getCopyCount();
            req.value = null;
            if (oldValue == null) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_ADDED, record);
            } else {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_UPDATED, record);
            }
            if (req.txnId != -1) {
                unlock(record);
            }
            updateIndexes(created, req, record);
            markAsDirty(record);
            return oldValue;
        }

        void updateIndexes(boolean created, Request request, Record record) {
            int valueHash = (record.getValue() != null) ? record.getValue().hashCode() : Integer.MIN_VALUE;
            if (request.indexes != null) {
                int indexCount = request.indexes.length;
                if (indexCount == 0)
                    throw new RuntimeException(node.getName() + " request countains no index " + request.indexes);
                if (mapIndexes.size() > indexCount) {
                    throw new RuntimeException(node.getName() + ": indexCount=" + indexCount + " but expected " + mapIndexes.size());
                }
                long[] newIndexes = request.indexes;
                request.indexes = null;
                byte[] indexTypes = request.indexTypes;
                request.indexTypes = null;
                if (newIndexes.length != indexTypes.length) {
                    throw new RuntimeException();
                }
                node.queryService.updateIndex(name, newIndexes, indexTypes, record, valueHash);
            } else if (created || record.getValueHash() != valueHash) {
                node.queryService.updateIndex(name, null, null, record, valueHash);
            }
        }

        void startAsyncStoreWrite() {
            logger.log(Level.FINEST, "startAsyncStoreWrite " + setDirtyRecords.size());
            long now = System.currentTimeMillis();
            Iterator<Record> itDirtyRecords = setDirtyRecords.iterator();
            final Map<Data, Data> entriesToStore = new HashMap<Data, Data>();
            final Collection<Data> keysToDelete = new HashSet<Data>();
            while (itDirtyRecords.hasNext()) {
                Record dirtyRecord = itDirtyRecords.next();
                if (dirtyRecord.getWriteTime() > now) {
                    if (dirtyRecord.getValue() != null) {
                        entriesToStore.put(doHardCopy(dirtyRecord.getKey()), doHardCopy(dirtyRecord.getValue()));
                    } else {
                        keysToDelete.add(doHardCopy(dirtyRecord.getKey()));
                    }
                    dirtyRecord.setDirty(false);
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
            if (setRemovedRecords.size() > 10) {
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
        }

        void startEviction() {
            List<Data> lsKeysToEvict = null;
            if (evictionPolicy == OrderingType.NONE) {
                if (ttl != 0) {
                    long now = System.currentTimeMillis();
                    Collection<Record> values = mapRecords.values();
                    for (Record record : values) {
                        if (record.isActive() && !record.isValid(now)) {
                            if (record.isEvictable()) {
                                if (lsKeysToEvict == null) {
                                    lsKeysToEvict = new ArrayList<Data>(100);
                                }
                                markAsRemoved(record);
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
                    if (record.isActive() && record.isEvictable()) {
                        if (lsKeysToEvict == null) {
                            lsKeysToEvict = new ArrayList<Data>(numberOfRecordsToEvict);
                        }
                        markAsRemoved(record);
                        lsKeysToEvict.add(doHardCopy(record.getKey()));
                        if (++evictedCount >= numberOfRecordsToEvict) {
                            break;
                        }
                    }
                }
            }
            if (lsKeysToEvict != null && lsKeysToEvict.size() > 0) {
                for (final Data key : lsKeysToEvict) {
                    executeLocally(new Runnable() {
                        public void run() {
                            try {
                                MEvict mEvict = new MEvict();
                                mEvict.evict(name, key);
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
                    fireMapEvent(mapListeners, name, EntryEvent.TYPE_EVICTED, record.getKey(), record.getValue(), record.getMapListeners());
                    removeAndPurgeRecord(record);
                    return true;
                }
            }
            return false;
        }

        final boolean ownerForSure(Record record) {
            Block block = blocks[record.getBlockId()];
            return block != null && !block.isMigrating() && thisAddress.equals(block.getOwner());
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
                    if (isMultiMap()) {
                        record.addValue(req.value);
                    } else {
                        record.setValue(req.value);
                    }
                }
            }
            req.value = null;
            record.setCopyCount((int) req.longValue);
            if (req.lockCount >= 0) {
                record.setLockAddress(req.lockAddress);
                record.setLockThreadId(req.lockThreadId);
                record.setLockCount(req.lockCount);
            }
            return record;
        }

        public boolean removeItem(Request req) {
            Record record = mapRecords.get(req.key);
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
            } else if (record.getMultiValues() != null) {
                removed = true;
            }
            if (removed) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record);
                record.setVersion(record.getVersion() + 1);
                if (record.getValue() != null) {
                    record.setValue(null);
                } else if (record.getMultiValues() != null) {
                    record.setMultiValues(null);
                }
            }
            req.version = record.getVersion();
            req.longValue = record.getCopyCount();
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
            if (oldValue == null && record.getMultiValues() != null && record.getMultiValues().size() > 0) {
                Values values = new Values(record.getMultiValues());
                oldValue = toData(values);
                record.setMultiValues(null);
            }
            if (oldValue != null) {
                fireMapEvent(mapListeners, name, EntryEvent.TYPE_REMOVED, record.getKey(), oldValue, record.getMapListeners());
                record.incrementVersion();
                record.setValue(null);
                record.setMultiValues(null);
            }
            req.version = record.getVersion();
            req.longValue = record.getCopyCount();
            if (record.isRemovable()) {
                markAsRemoved(record);
            }
            if (oldValue != null) {
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
            if (setDirtyRecords != null) {
                setDirtyRecords.clear();
            }
            setRemovedRecords.clear();
            node.queryService.reset(name);
        }

        void scheduleForEviction(Record record) {
            SortedHashMap.moveToTop(mapRecords, record.getKey());
        }

        void touch(Record record) {
            record.setLastTouchTime(System.currentTimeMillis());
            if (evictionPolicy == OrderingType.NONE) return;
            SortedHashMap.touch(mapRecords, record.getKey(), evictionPolicy);
        }

        void markAsDirty(Record record) {
            if (!record.isDirty()) {
                record.setDirty(true);
                if (writeDelaySeconds > 0) {
                    record.setWriteTime(System.currentTimeMillis() + (writeDelaySeconds * 1000L));
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
            return !record.isActive() && ((now - record.getRemoveTime()) > removeDelayMillis);
        }

        void markAsRemoved(Record record) {
            if (record.isActive()) {
                record.markRemoved();
                setRemovedRecords.add(record);
                Block ownerBlock = blocks[record.getBlockId()];
                if (thisAddress.equals(ownerBlock.getRealOwner())) {
                    ownedEntryCount--;
                }
            }
        }

        void removeAndPurgeRecord(Record record) {
            Block ownerBlock = blocks[record.getBlockId()];
            if (thisAddress.equals(ownerBlock.getRealOwner())) {
                node.queryService.updateIndex(name, null, null, record, Integer.MIN_VALUE);
            }
            Record removedRecord = mapRecords.remove(record.getKey());
            if (removedRecord != record) {
                throw new RuntimeException(removedRecord + " is removed but should have removed " + record);
            }
        }

        Record createNewRecord(Data key, Data value) {
            if (key == null || key.size() == 0) {
                throw new RuntimeException("Cannot create record from a 0 size key: " + key);
            }
            int blockId = getBlockId(key);
            Record rec = new Record(node.factory, name, blockId, key, value, ttl, newRecordId++);
            Block ownerBlock = getOrCreateBlock(blockId);
            if (thisAddress.equals(ownerBlock.getRealOwner())) {
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

    public void fireScheduledActions(Record record) {
        checkServiceThread();
        if (record.getLockCount() == 0) {
            record.setLockThreadId(-1);
            record.setLockAddress(null);
            if (record.getLsScheduledActions() != null) {
                while (record.getLsScheduledActions().size() > 0) {
                    ScheduledAction sa = record.getLsScheduledActions().remove(0);
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
        List<ScheduledAction> lsScheduledActions = record.getLsScheduledActions();
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
        if (record.getLockCount() > 0) {
            if (deadAddress.equals(record.getLockAddress())) {
                unlock(record);
            }
        }
    }

    public void unlock(Record record) {
        record.setLockThreadId(-1);
        record.setLockCount(0);
        record.setLockAddress(null);
        fireScheduledActions(record);
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
            TransactionImpl txn = ThreadContext.get().callContext.txn;
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
            TransactionImpl txn = ThreadContext.get().callContext.txn;
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

        public List<Map.Entry> getLsKeyValues() {
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
                Data data = new Data();
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
