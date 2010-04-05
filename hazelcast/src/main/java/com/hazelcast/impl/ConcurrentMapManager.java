/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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

import com.hazelcast.core.MapEntry;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.Transaction;
import com.hazelcast.impl.base.*;
import com.hazelcast.impl.concurrentmap.MultiData;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryContext;
import com.hazelcast.util.ResponseQueueFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.Instance.InstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.Constants.Timeouts.DEFAULT_TXN_TIMEOUT;
import static com.hazelcast.impl.LocalMapStatsImpl.Op.UNLOCK;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;

public class ConcurrentMapManager extends BaseManager {
    final int PARTITION_COUNT;
    final long GLOBAL_REMOVE_DELAY_MILLIS;
    final long CLEANUP_DELAY_MILLIS;
    final boolean LOG_STATE;
    long lastLogStateTime = System.currentTimeMillis();
    final Block[] blocks;
    final ConcurrentMap<String, CMap> maps;
    final ConcurrentMap<String, LocallyOwnedMap> mapLocallyOwnedMaps;
    final ConcurrentMap<String, MapNearCache> mapCaches;
    final OrderedExecutionTask[] orderedExecutionTasks;
    final PartitionManager partitionManager;
    long newRecordId = 0;
    volatile long nextCleanup = 0;

    ConcurrentMapManager(Node node) {
        super(node);
        PARTITION_COUNT = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        GLOBAL_REMOVE_DELAY_MILLIS = node.groupProperties.REMOVE_DELAY_SECONDS.getLong() * 1000L;
        CLEANUP_DELAY_MILLIS = node.groupProperties.CLEANUP_DELAY_SECONDS.getLong() * 1000L;
        LOG_STATE = node.groupProperties.LOG_STATE.getBoolean();
        blocks = new Block[PARTITION_COUNT];
        maps = new ConcurrentHashMap<String, CMap>(10);
        mapLocallyOwnedMaps = new ConcurrentHashMap<String, LocallyOwnedMap>(10);
        mapCaches = new ConcurrentHashMap<String, MapNearCache>(10);
        orderedExecutionTasks = new OrderedExecutionTask[PARTITION_COUNT];
        partitionManager = new PartitionManager(this);
        for (int i = 0; i < PARTITION_COUNT; i++) {
            orderedExecutionTasks[i] = new OrderedExecutionTask();
        }
        node.clusterService.registerPeriodicRunnable(new Runnable() {
            public void run() {
                logState();
                long now = System.currentTimeMillis();
                if (now > nextCleanup) {
                    nextCleanup = Long.MAX_VALUE;
                    executeLocally(new Runnable() {
                        public void run() {
                            Collection<CMap> cmaps = maps.values();
                            for (CMap cmap : cmaps) {
                                cmap.startCleanup();
                            }
                            nextCleanup = System.currentTimeMillis() + CLEANUP_DELAY_MILLIS;
                        }
                    });
                }
            }
        });
        node.clusterService.registerPeriodicRunnable(partitionManager);
        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEnryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_TRY_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_IF_ABSENT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_SAME, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_MULTI, new PutMultiOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE, new RemoveOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_EVICT, new EvictOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_IF_SAME, new RemoveOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_ITEM, new RemoveItemOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_ADD, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE_MULTI, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_LOCK, new BackupPacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_LOCK, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_LOCK_AND_GET_VALUE, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_UNLOCK, new UnlockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_LOCK_MAP, new LockMapOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_UNLOCK_MAP, new LockMapOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_ENTRIES, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_VALUES, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ITERATE_KEYS_ALL, new QueryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MIGRATE_RECORD, new MigrationOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_MULTI, new RemoveMultiOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_LIST, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_SET, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_SIZE, new SizeOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS, new ContainsOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_VALUE, new ContainsValueOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCK_INFO, new BlockInfoOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCKS, new BlocksOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BLOCK_MIGRATION_CHECK, new BlockMigrationCheckHandler());
        registerPacketProcessor(CONCURRENT_MAP_VALUE_COUNT, new ValueCountOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_INVALIDATE, new InvalidateOperationHandler());
    }

    public void reset() {
        maps.clear();
    }

    public void syncForDead(MemberImpl deadMember) {
        partitionManager.syncForDead(deadMember);
    }

    public void syncForAdd() {
        partitionManager.syncForAdd();
    }

    public int hashBlocks() {
        int hash = 1;
        for (int i = 0; i < PARTITION_COUNT; i++) {
            Block block = blocks[i];
            hash = (hash * 31) + ((block == null) ? 0 : block.customHash());
        }
        return hash;
    }

    void logState() {
        long now = System.currentTimeMillis();
        if (LOG_STATE && ((now - lastLogStateTime) > 15000)) {
            StringBuffer sbState = new StringBuffer(thisAddress + " State[" + new Date(now));
            sbState.append("]======================");
            for (Block block : blocks) {
                if (block != null && block.isMigrating()) {
                    sbState.append("\n");
                    sbState.append(block);
                }
            }
            Collection<Call> calls = mapCalls.values();
            for (Call call : calls) {
                if (call.getEnqueueCount() > 15 || (now - call.getFirstEnqueueTime() > 15000)) {
                    sbState.append("\n");
                    sbState.append(call);
                }
            }
            sbState.append("\nCall Count:" + calls.size());
            Collection<CMap> cmaps = maps.values();
            for (CMap cmap : cmaps) {
                cmap.appendState(sbState);
            }
            node.blockingQueueManager.appendState(sbState);
            node.executorManager.appendState(sbState);
            long total = Runtime.getRuntime().totalMemory();
            long free = Runtime.getRuntime().freeMemory();
            sbState.append("\nCluster Size:" + lsMembers.size());
            sbState.append("\nUsed Memory:");
            sbState.append((total - free) / 1024 / 1024);
            sbState.append("MB");
            logger.log(Level.INFO, sbState.toString());
            lastLogStateTime = now;
        }
    }

    void backupRecord(final Record rec) {
        if (rec.getMultiValues() != null) {
            Set<Data> values = rec.getMultiValues();
            int initialVersion = ((int) rec.getVersion() - values.size());
            int version = (initialVersion < 0) ? 0 : initialVersion;
            for (Data value : values) {
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
        if (!node.isActive() || node.factory.restarted) return;
        MMigrate mmigrate = new MMigrate();
        if (cmap.isMultiMap()) {
            Set<Data> values = rec.getMultiValues();
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

    public boolean isOwned(Record record) {
        Block block = getOrCreateBlock(record.getBlockId());
        return thisAddress.equals(block.getOwner());
    }

    public int getPartitionCount() {
        return PARTITION_COUNT;
    }

    public Block[] getBlocks() {
        return blocks;
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

        public boolean lockAndGetValue(String name, Object key, long timeout) {
            oldValue = null;
            boolean locked = booleanCall(CONCURRENT_MAP_LOCK_AND_GET_VALUE, name, key, null, timeout, -1);
            if (locked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
            }
            return locked;
        }

        @Override
        public void afterGettingResult(Request request) {
            if (request.operation == CONCURRENT_MAP_LOCK_AND_GET_VALUE) {
                oldValue = request.value;
            }
            super.afterGettingResult(request);
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            if (request.operation == CONCURRENT_MAP_LOCK_AND_GET_VALUE) {
                oldValue = packet.value;
            }
            super.handleNoneRedoResponse(packet);
        }
    }

    class MContainsKey extends MTargetAwareOp {
        Object key = null;
        MapNearCache nearCache = null;

        public boolean containsEntry(String name, Object key, Object value) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, value, 0, -1);
        }

        public boolean containsKey(String name, Object key) {
            this.key = key;
            this.nearCache = mapCaches.get(name);
            Data dataKey = toData(key);
            if (nearCache != null) {
                if (nearCache.containsKey(key)) {
                    return true;
                } else if (nearCache.getMaxSize() == Integer.MAX_VALUE) {
                    return false;
                }
            }
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, dataKey, null, 0, -1);
        }

        @Override
        public void reset() {
            key = null;
            nearCache = null;
            super.reset();
        }

        @Override
        protected void setResult(Object obj) {
            if (obj != null && obj == Boolean.TRUE) {
                if (nearCache != null) {
                    nearCache.setContainsKey(key, request.key);
                }
            }
            super.setResult(obj);
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
    }

    class MMigrate extends MBackupAwareOp {

        public boolean migrateMulti(Record record, Data value) {
            request.setFromRecord(record);
            request.value = value;
            request.operation = CONCURRENT_MAP_MIGRATE_RECORD;
            request.setBooleanRequest();
            doOp();
            boolean result = getResultAsBoolean();
            backup(CONCURRENT_MAP_BACKUP_PUT);
            return result;
        }

        public boolean migrate(Record record) {
            request.setFromRecord(record);
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
            target = getKeyOwner(request.key);
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
            Object result = objectCall(CONCURRENT_MAP_GET_MAP_ENTRY, name, key, null, 0, -1);
            if (result instanceof Data) {
                result = toObject((Data) result);
            }
            CMap.CMapEntry mapEntry = (CMap.CMapEntry) result;
            if (mapEntry != null) {
                mapEntry.set(name, key);
            }
            return mapEntry;
        }
    }

    class MAddKeyListener extends MTargetAwareOp {

        public boolean addListener(String name, boolean add, Object key, boolean includeValue) {
            ClusterOperation operation = (add) ? ADD_LISTENER : REMOVE_LISTENER;
            setLocal(operation, name, key, null, -1, -1);
            request.longValue = (includeValue) ? 1 : 0;
            request.setBooleanRequest();
            doOp();
            return getResultAsBoolean();
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    class MGet extends MTargetAwareOp {
        Object key = null;
        MapNearCache nearCache = null;

        public Object get(String name, Object key, long timeout) {
            this.key = key;
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (txn.has(name, key)) {
                    return txn.get(name, key);
                }
            }
            nearCache = mapCaches.get(name);
            if (nearCache != null) {
                Object value = nearCache.get(key);
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
        public void reset() {
            key = null;
            nearCache = null;
            super.reset();
        }

        @Override
        public final void handleNoneRedoResponse(Packet packet) {
            if (nearCache != null) {
                Data value = packet.value;
                if (value != null && value.size() > 0) {
                    nearCache.put(this.key, request.key, packet.value);
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
                                .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
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
                                .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
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

    class MPutMulti extends MBackupAndMigrationAwareOp {

        boolean put(String name, Object key, Object value) {
            boolean result = booleanCall(CONCURRENT_MAP_PUT_MULTI, name, key, value, 0, -1);
            if (result) {
                backup(CONCURRENT_MAP_BACKUP_PUT);
            }
            return result;
        }
    }

    boolean isMapIndexed(String name) {
        CMap cmap = getMap(name);
        return cmap != null && (cmap.getMapIndexService().hasIndexedAttributes());
    }

    void setIndexValues(Request request, Object value) {
        CMap cmap = getMap(request.name);
        if (cmap != null) {
            long[] indexes = cmap.getMapIndexService().getIndexValues(value);
            if (indexes != null) {
                byte[] indexTypes = cmap.getMapIndexService().getIndexTypes();
                request.setIndexes(indexes, indexTypes);
                for (byte b : indexTypes) {
                    if (b == -1) {
                        throw new RuntimeException("Index type cannot be -1: " + b);
                    }
                }
            }
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

        public boolean tryPut(String name, Object key, Object value, long timeout, long ttl) {
            return (Boolean) txnalPut(CONCURRENT_MAP_TRY_PUT, name, key, value, timeout, ttl);
        }

        private Object txnalReplaceIfSame(ClusterOperation operation, String name, Object key, Object newValue, Object expectedValue, long timeout) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
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
                            .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
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
                if (operation == CONCURRENT_MAP_TRY_PUT) {
                    request.setBooleanRequest();
                    doOp();
                    Boolean returnObject = getResultAsBoolean();
                    if (returnObject) {
                        backup(CONCURRENT_MAP_BACKUP_PUT);
                    }
                    return returnObject;
                } else {
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

    class MBackup extends MTargetAwareOp {
        protected Address owner = null;
        protected int distance = 0;

        public void sendBackup(ClusterOperation operation, Address owner, int distance, Request reqBackup) {
            reset();
            this.owner = owner;
            this.distance = distance;
            request.setFromRequest(reqBackup);
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

    class MBackupOp extends MBackupAwareOp {
        public void backup(Record record) {
            request.setFromRecord(record);
            doOp();
            boolean stillOwner = getResultAsBoolean();
            if (stillOwner) {
                target = thisAddress;
                backup(CONCURRENT_MAP_BACKUP_PUT);
            }
        }

        @Override
        public void process() {
            prepareForBackup();
            if (!thisAddress.equals(getKeyOwner(request.key))) {
                setResult(Boolean.FALSE);
            } else {
                setResult(Boolean.TRUE);
            }
        }
    }

    abstract class MBackupAwareOp extends MTargetAwareOp {
        protected final MBackup[] backupOps = new MBackup[3];
        protected volatile int backupCount = 0;

        protected void backup(ClusterOperation operation) {
            if (backupCount > 0) {
                for (int i = 0; i < backupCount; i++) {
                    int distance = i + 1;
                    MBackup backupOp = backupOps[i];
                    if (backupOp == null) {
                        backupOp = new MBackup();
                        backupOps[i] = backupOp;
                    }
                    if (request.key == null || request.key.size() == 0) {
                        throw new RuntimeException("Key is null! " + request.key);
                    }
                    backupOp.sendBackup(operation, target, distance, request);
                }
                for (int i = 0; i < backupCount; i++) {
                    MBackup backupOp = backupOps[i];
                    backupOp.getResultAsBoolean();
                }
            }
        }

        void prepareForBackup() {
            backupCount = 0;
            if (lsMembers.size() > 1) {
                CMap map = getOrCreateMap(request.name);
                backupCount = map.getBackupCount();
                backupCount = Math.min(backupCount, lsMembers.size());
            }
        }

        @Override
        public void process() {
            prepareForBackup();
            super.process();
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            handleRemoteResponse(packet);
            super.handleNoneRedoResponse(packet);
        }

        public void handleRemoteResponse(Packet packet) {
            request.local = true;
            request.version = packet.version;
            request.lockCount = packet.lockCount;
            request.longValue = packet.longValue;
        }
    }

    abstract class MMigrationAwareTargetedCall extends MigrationAwareTargetedCall {

        @Override
        public void process() {
            request.blockId = hashBlocks();
            super.process();
        }
    }

    void fireMapEvent(final Map<Address, Boolean> mapListeners, final String name,
                      final int eventType, final Record record) {
        checkServiceThread();
        fireMapEvent(mapListeners, name, eventType, record.getKey(), record.getValue(), record.getMapListeners());
    }

    public class MContainsValue extends MultiCall<Boolean> {
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

        Boolean returnResult() {
            return contains;
        }

        class MGetContainsValue extends MMigrationAwareTargetedCall {
            public MGetContainsValue(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_CONTAINS_VALUE, name, null, value, 0, -1);
                request.setBooleanRequest();
            }
        }
    }

    public class MLockMap extends MultiCall<Boolean> {
        private final String name;
        private final long timeout;
        private final ClusterOperation operation;

        public MLockMap(String name, boolean lock, long timeout) {
            this.name = name;
            this.operation = (lock) ? CONCURRENT_MAP_LOCK_MAP : CONCURRENT_MAP_UNLOCK_MAP;
            this.timeout = timeout;
        }

        TargetAwareOp createNewTargetAwareOp(Address target) {
            return new MTargetLockMap(target);
        }

        boolean onResponse(Object response) {
            return true;
        }

        void onCall() {
        }

        Boolean returnResult() {
            return true;
        }

        class MTargetLockMap extends MMigrationAwareTargetedCall {
            public MTargetLockMap(Address target) {
                this.target = target;
                request.reset();
                setLocal(operation, name, null, null, timeout, -1);
                request.setBooleanRequest();
            }
        }
    }

    public class MSize extends MultiCall<Integer> {
        int size = 0;
        final String name;

        public int getSize() {
            int size = call();
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

        Integer returnResult() {
            return size;
        }

        class MGetSize extends MMigrationAwareTargetedCall {
            public MGetSize(Address target) {
                this.target = target;
                request.reset();
                setLocal(CONCURRENT_MAP_SIZE, name);
                request.setLongRequest();
            }
        }
    }

    public LocalMapStatsImpl getLocalMapStats(String name) {
        final CMap cmap = getMap(name);
        if (cmap == null) {
            return new LocalMapStatsImpl();
        }
        int tryCount = 0;
        while (tryCount++ < 10 && partitionManager.partitionServiceImpl.isMigrating()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        final BlockingQueue responseQ = ResponseQueueFactory.newResponseQueue();
        enqueueAndReturn(new Processable() {
            public void process() {
                responseQ.offer(cmap.getLocalMapStats());
            }
        });
        try {
            return (LocalMapStatsImpl) responseQ.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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
            } else if (response instanceof Pairs) {
                pairs = (Pairs) response;
            } else {
                // null
                return true;
            }
            entries.addEntries(pairs);
            return true;
        }

        Object returnResult() {
            return entries;
        }
    }

    class MGetEntries extends MMigrationAwareTargetedCall {
        public MGetEntries(Address target, ClusterOperation operation, String name, Predicate predicate) {
            this.target = target;
            request.reset();
            setLocal(operation, name, null, predicate, -1, -1);
        }
    }

    public Address getKeyOwner(Data key) {
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
        return partitionManager.isMigrating(req);
    }

    public int getBlockId(Data key) {
        int hash = key.hashCode();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % PARTITION_COUNT;
    }

    public long newRecordId() {
        return newRecordId++;
    }

    Block getOrCreateBlock(Data key) {
        return getOrCreateBlock(getBlockId(key));
    }

    void evictAsync(final String name, final Data key) {
        executeLocally(new FallThroughRunnable() {
            public void doRun() {
                try {
                    MEvict mEvict = new MEvict();
                    mEvict.evict(name, key);
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
            if (isMaster() && !isSuperClient()) {
                block.setOwner(thisAddress);
            }
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
    void handleListenerRegistrations(boolean add, String name, Data key, Address address, boolean includeValue) {
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

    class BlockMigrationCheckHandler extends AbstractOperationHandler {
        @Override
        void doOperation(Request request) {
            request.response = partitionManager.containsMigratingBlock();
        }
    }

    class BlocksOperationHandler extends BlockInfoOperationHandler {

        @Override
        public void process(Packet packet) {
            Blocks blocks = (Blocks) toObject(packet.value);
            partitionManager.handleBlocks(blocks);
            releasePacket(packet);
        }
    }

    class BlockInfoOperationHandler implements PacketProcessor {

        public void process(Packet packet) {
            Block blockInfo = (Block) toObject(packet.value);
            partitionManager.completeMigration(blockInfo.getBlockId());
            if (isMaster() && !blockInfo.isMigrating()) {
                for (MemberImpl member : lsMembers) {
                    if (!member.localMember()) {
                        if (!member.getAddress().equals(packet.conn.getEndPoint())) {
                            sendBlockInfo(new Block(blockInfo), member.getAddress());
                        }
                    }
                }
            }
            releasePacket(packet);
        }
    }

    class LockMapOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.lockMap(request);
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
            releasePacket(packet);
        }
    }

    abstract class MTargetAwareOperationHandler extends TargetAwareOperationHandler {
        boolean isRightRemoteTarget(Request request) {
            return thisAddress.equals(getKeyOwner(request.key));
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

    class RemoveMultiOperationHandler extends SchedulableOperationHandler {
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
        @Override
        protected void onNoTimeToSchedule(Request request) {
            request.response = null;
            if (request.operation == CONCURRENT_MAP_TRY_PUT) {
                request.response = Boolean.FALSE;
            }
            returnResponse(request);
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.put(request);
            if (request.operation == CONCURRENT_MAP_TRY_PUT) {
                request.response = Boolean.TRUE;
            }
        }
    }

    class AddOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.add(request, false);
        }
    }

    class GetMapEnryOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.getMapEntry(request);
        }
    }

    class EvictOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            if (checkMapLock(request)) {
                CMap cmap = getOrCreateMap(request.name);
                Record record = cmap.getRecord(request.key);
                if (record != null && record.isActive() && cmap.loader != null &&
                        cmap.writeDelayMillis > 0 && record.isValid() && record.isDirty()) {
                    // if the map has write-behind and the record is dirty then
                    // we have to make sure that the entry is actually persisted
                    // before we can evict it.
                    record.setDirty(false);
                    request.value = record.getValue();
                    executeAsync(request);
                } else {
                    doOperation(request);
                    returnResponse(request);
                }
            } else {
                request.response = OBJECT_REDO;
                returnResponse(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.evict(request);
        }

        public void afterExecute(Request request) {
            if (request.response == Boolean.TRUE) {
                doOperation(request);
            } else {
                CMap cmap = getOrCreateMap(request.name);
                Record record = cmap.getRecord(request.key);
                cmap.markAsDirty(record);
                request.response = Boolean.FALSE;
            }
            returnResponse(request);
        }
    }

    class GetOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            if (checkMapLock(request)) {
                CMap cmap = getOrCreateMap(request.name);
                Record record = cmap.getRecord(request.key);
                if (cmap.loader != null && (record == null || !record.isActive() || record.getValue() == null)) {
                    executeAsync(request);
                } else {
                    doOperation(request);
                    returnResponse(request);
                }
            } else {
                request.response = OBJECT_REDO;
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
                    CMap cmap = getOrCreateMap(request.name);
                    cmap.markAsActive(record);
                    cmap.updateIndexes(record);
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
                    CMap cmap = getOrCreateMap(record.getName());
                    record.incrementVersion();
                    request.version = record.getVersion();
                    request.lockCount = record.getLockCount();
                    cmap.fireScheduledActions(record);
                    cmap.updateStats(UNLOCK, record, true, null);
                }
                logger.log(Level.FINEST, unlocked + " now lock lockCount " + record.getLockCount() + " lockThreadId " + record.getLockThreadId());
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
            request.response = Boolean.FALSE;
            returnResponse(request);
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.lock(request);
        }
    }

    abstract class SchedulableOperationHandler extends MTargetAwareOperationHandler {

        protected boolean shouldSchedule(Request request) {
            return (!testLock(request));
        }

        protected void onNoTimeToSchedule(Request request) {
            request.response = null;
            returnResponse(request);
        }

        protected void schedule(Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            final Record record = ensureRecord(request);
            request.scheduled = true;
            ScheduledAction scheduledAction = new ScheduledAction(request) {
                @Override
                public boolean consume() {
                    cmap.decrementLockWaits(record);
                    handle(request);
                    return true;
                }

                @Override
                public void onExpire() {
                    cmap.decrementLockWaits(record);
                    onNoTimeToSchedule(request);
                }
            };
            cmap.incrementLockWaits(record);
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
         * executorThread
         */
        void execute(Request request);

        /**
         * serviceThread
         */
        void afterExecute(Request request);
    }

    void executeAsync(Request request) {
        OrderedExecutionTask orderedExecutionTask = orderedExecutionTasks[getBlockId(request.key)];
        int size = orderedExecutionTask.offer(request);
        if (size == 1) {
            node.executorManager.executeStoreTask(orderedExecutionTask);
        }
    }

    abstract class StoreAwareOperationHandler extends SchedulableOperationHandler implements AsynchronousExecution {

        /**
         * executorThread
         */
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
            } else if (request.operation == CONCURRENT_MAP_EVICT) {
                //store the entry
                cmap.store.store(toObject(request.key), toObject(request.value));
                request.response = Boolean.TRUE;
            }
        }

        protected boolean shouldExecuteAsync(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            return (cmap.writeDelayMillis == 0);
        }

        public void handle(Request request) {
            if (checkMapLock(request)) {
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
            } else {
                request.response = OBJECT_REDO;
                returnResponse(request);
            }
        }

        /**
         * serviceThread
         */
        public void afterExecute(Request request) {
            if (request.response == null) {
                doOperation(request);
            } else {
                request.value = (Data) request.response;
            }
            returnResponse(request);
        }
    }

    class ContainsValueOperationHandler extends AbstractOperationHandler {

        @Override
        public void process(Packet packet) {
            processMigrationAware(packet);
        }

        @Override
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.containsValue(request);
        }
    }

    abstract class ExecutedOperationHandler extends ResponsiveOperationHandler {
        public void process(Packet packet) {
            Request request = Request.copy(packet);
            request.local = false;
            handle(request);
        }

        public void handle(final Request request) {
            node.executorManager.executeQueryTask(createRunnable(request));
        }

        abstract Runnable createRunnable(Request request);
    }

    public boolean checkMapLock(Request request) {
        CMap cmap = getOrCreateMap(request.name);
        return cmap.checkLock(request);
    }

    class SizeOperationHandler extends ExecutedOperationHandler {
        @Override
        public void handle(Request request) {
            if (checkMapLock(request) && !isMigrating(request)) {
                super.handle(request);
            } else {
                request.response = OBJECT_REDO;
                returnResponse(request);
            }
        }

        @Override
        Runnable createRunnable(final Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            return new Runnable() {
                public void run() {
                    request.response = (long) cmap.size();
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            int callerPartitionHash = request.blockId;
                            int myPartitionHashNow = hashBlocks();
                            if (callerPartitionHash != myPartitionHashNow) {
                                request.response = OBJECT_REDO;
                            }
                            boolean sent = returnResponse(request);
                            if (!sent) {
                                Connection conn = node.connectionManager.getConnection(request.caller);
                                logger.log(Level.WARNING, request + " !! response cannot be sent to "
                                        + request.caller + " conn:" + conn);
                            }
                        }
                    });
                }
            };
        }
    }

    class QueryOperationHandler extends ExecutedOperationHandler {

        @Override
        public void handle(Request request) {
            if (checkMapLock(request) && !isMigrating(request)) {
                super.handle(request);
            } else {
                request.response = OBJECT_REDO;
                returnResponse(request);
            }
        }

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
                try {
                    Predicate predicate = null;
                    if (request.value != null) {
                        predicate = (Predicate) toObject(request.value);
                    }
                    QueryContext queryContext = new QueryContext(cmap.getName(), predicate);
                    Set<MapEntry> results = cmap.getMapIndexService().doQuery(queryContext);
                    boolean evaluateValues = (predicate != null && !queryContext.isStrong());
                    createResultPairs(request, results, evaluateValues, predicate);
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            int callerPartitionHash = request.blockId;
                            if (partitionManager.containsMigratingBlock() || callerPartitionHash != hashBlocks()) {
                                request.response = OBJECT_REDO;
                            }
                            boolean sent = returnResponse(request);
                            if (!sent) {
                                Connection conn = node.connectionManager.getConnection(request.caller);
                                logger.log(Level.WARNING, request + " !! response cannot be sent to "
                                        + request.caller + " conn:" + conn);
                            }
                        }
                    });
                } catch (Throwable e) {
                    logger.log(Level.SEVERE, request.toString(), e);
                }
            }
        }

        void createResultPairs(Request request, Collection<MapEntry> colRecords, boolean evaluateEntries, Predicate predicate) {
            Pairs pairs = new Pairs();
            if (colRecords != null) {
                long now = System.currentTimeMillis();
                for (MapEntry mapEntry : colRecords) {
                    Record record = (Record) mapEntry;
                    if (record.isActive() && record.isValid(now)) {
                        if (record.getKey() == null || record.getKey().size() == 0) {
                            throw new RuntimeException("Key cannot be null or zero-size: " + record.getKey());
                        }
                        boolean match = (!evaluateEntries) || predicate.apply(record.getRecordEntry());
                        if (match) {
                            boolean onlyKeys = (request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL ||
                                    request.operation == CONCURRENT_MAP_ITERATE_KEYS);
                            Data key = record.getKey();
                            if (record.getValue() != null) {
                                Data value = (onlyKeys) ? null : record.getValue();
                                pairs.addKeyValue(new KeyValue(key, value));
                            } else if (record.getCopyCount() > 0) {
                                for (int i = 0; i < record.getCopyCount(); i++) {
                                    pairs.addKeyValue(new KeyValue(key, null));
                                }
                            } else if (record.getMultiValues() != null) {
                                int size = record.getMultiValues().size();
                                if (size > 0) {
                                    if (request.operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                        pairs.addKeyValue(new KeyValue(key, null));
                                    } else {
                                        Set<Data> values = record.getMultiValues();
                                        for (Data value : values) {
                                            pairs.addKeyValue(new KeyValue(key, value));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            request.response = (pairs.size() > 0) ? ((request.local) ? pairs : toData(pairs)) : null;
        }
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
            cmap.mapRecords.put(req.key, record);
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
                        logger.log(Level.FINEST, "Store thrown exception for " + request.operation, e);
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
            if (pairs == null) return;
            if (pairs.getKeyValues() == null) return;
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            for (KeyValue entry : pairs.getKeyValues()) {
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
                    ((IRemoveAwareProxy) node.factory.getOrCreateProxyByName(name)).removeKey(entry.getKey());
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
