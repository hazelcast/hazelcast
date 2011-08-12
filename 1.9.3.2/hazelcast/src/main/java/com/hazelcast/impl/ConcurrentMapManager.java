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

import com.hazelcast.core.*;
import com.hazelcast.impl.base.*;
import com.hazelcast.impl.concurrentmap.MultiData;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.nio.*;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryContext;
import com.hazelcast.util.DistributedTimeoutException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.hazelcast.core.Instance.InstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.Objects.OBJECT_REDO;
import static com.hazelcast.impl.TransactionImpl.DEFAULT_TXN_TIMEOUT;
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
    final ConcurrentMap<String, NearCache> mapCaches;
    final OrderedExecutionTask[] orderedExecutionTasks;
    final PartitionManager partitionManager;
    long newRecordId = 0;
    volatile long nextCleanup = 0;
    final ParallelExecutor storeExecutor;

    ConcurrentMapManager(Node node) {
        super(node);
        storeExecutor = node.executorManager.newParallelExecutor(node.groupProperties.EXECUTOR_STORE_THREAD_COUNT.getInteger());
        PARTITION_COUNT = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        GLOBAL_REMOVE_DELAY_MILLIS = node.groupProperties.REMOVE_DELAY_SECONDS.getLong() * 1000L;
        CLEANUP_DELAY_MILLIS = node.groupProperties.CLEANUP_DELAY_SECONDS.getLong() * 1000L;
        LOG_STATE = node.groupProperties.LOG_STATE.getBoolean();
        blocks = new Block[PARTITION_COUNT];
        maps = new ConcurrentHashMap<String, CMap>(10, 0.75f, 1);
        mapCaches = new ConcurrentHashMap<String, NearCache>(10, 0.75f, 1);
        orderedExecutionTasks = new OrderedExecutionTask[PARTITION_COUNT];
        partitionManager = new PartitionManager(this);
        for (int i = 0; i < PARTITION_COUNT; i++) {
            orderedExecutionTasks[i] = new OrderedExecutionTask();
        }
        node.clusterService.registerPeriodicRunnable(new FallThroughRunnable() {
            public void doRun() {
                logState();
                long now = System.currentTimeMillis();
                Collection<CMap> cmaps = maps.values();
                for (final CMap cmap : cmaps) {
                    if (cmap.cleanupState == CMap.CleanupState.SHOULD_CLEAN) {
                        executeCleanup(cmap, true);
                    }
                }
                if (now > nextCleanup) {
                    for (final CMap cmap : cmaps) {
                        if (cmap.cleanupState == CMap.CleanupState.NONE) {
                            executeCleanup(cmap, false);
                        }
                    }
                    nextCleanup = now + CLEANUP_DELAY_MILLIS;
                }
            }
        });
        node.clusterService.registerPeriodicRunnable(partitionManager);
        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEntryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_MERGE, new MergeOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_TRY_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_SET, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_AND_UNLOCK, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_TRANSIENT, new PutTransientOperationHandler());
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
        registerPacketProcessor(CONCURRENT_MAP_TRY_LOCK_AND_GET, new LockOperationHandler());
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
        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_SET, new AtomicOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_ADD, new AtomicOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_COMPARE_AND_SET, new AtomicOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_ADD_AND_GET, new AtomicOperationHandler());
        registerPacketProcessor(SEMAPHORE_ACQUIRE, new SemaphoreOperationHandler());
        registerPacketProcessor(SEMAPHORE_RELEASE, new SemaphoreOperationHandler());
        registerPacketProcessor(SEMAPHORE_AVAILABLE_PERIMITS, new SemaphoreOperationHandler());
        registerPacketProcessor(SEMAPHORE_DRAIN_PERIMITS, new SemaphoreOperationHandler());
    }

    private void executeCleanup(final CMap cmap, final boolean forced) {
        if (cmap.cleanupState == CMap.CleanupState.CLEANING) {
            return;
        }
        cmap.cleanupState = CMap.CleanupState.CLEANING;
        executeLocally(new FallThroughRunnable() {
            public void doRun() {
                try {
                    cmap.startCleanup(forced);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.log(Level.SEVERE, e.getMessage(), e);
                } finally {
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            cmap.cleanupState = CMap.CleanupState.NONE;
                        }
                    });
                }
            }
        });
    }

    public void onRestart() {
        enqueueAndWait(new Processable() {
            public void process() {
                partitionManager.reset();
                for (CMap cmap : maps.values()) {
                    cmap.reset();
                }
            }
        }, 5);
    }

    public void reset() {
        maps.clear();
        mapCaches.clear();
        partitionManager.reset();
    }

    public void shutdown() {
        for (CMap cmap : maps.values()) {
            try {
                flush(cmap.name);
            } catch (Throwable e) {
                if (node.isActive()) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
    }

    public void flush(String name) {
        CMap cmap = getMap(name);
        if (cmap != null && cmap.store != null && cmap.writeDelayMillis > 0) {
            Map mapDirtyEntries = new HashMap();
            for (Record record : cmap.mapRecords.values()) {
                if (record.isDirty()) {
                    Object key = record.getKey();
                    Object value = record.getValue();
                    if (key != null && value != null) {
                        mapDirtyEntries.put(key, value);
                    }
                }
            }
            if (mapDirtyEntries.size() > 0) {
                cmap.store.storeAll(mapDirtyEntries);
            }
        }
    }

    public void syncForDead(MemberImpl deadMember) {
        partitionManager.syncForDead(deadMember);
    }

    public void syncForAdd() {
        partitionManager.syncForAdd();
    }

    void logState() {
        long now = System.currentTimeMillis();
        if (LOG_STATE && ((now - lastLogStateTime) > 15000)) {
            StringBuffer sbState = new StringBuffer(thisAddress + " State[" + new Date(now));
            sbState.append("]");
            Collection<Call> calls = mapCalls.values();
//            for (Call call : calls) {
//                if (call.getEnqueueCount() > 15 || (now - call.getFirstEnqueueTime() > 15000)) {
//                    sbState.append("\n");
//                    sbState.append(call);
//                }
//            }
            sbState.append("\nCall Count:" + calls.size());
            for (Block block : blocks) {
                if (block != null && block.isMigrating()) {
                    sbState.append("\n");
                    sbState.append(block);
                }
            }
            Collection<CMap> cmaps = maps.values();
            for (CMap cmap : cmaps) {
                cmap.appendState(sbState);
            }
            CpuUtilization cpuUtilization = node.getCpuUtilization();
            node.connectionManager.appendState(sbState);
            node.executorManager.appendState(sbState);
            node.clusterManager.appendState(sbState);
            long total = Runtime.getRuntime().totalMemory();
            long free = Runtime.getRuntime().freeMemory();
            sbState.append("\nCluster Size:" + lsMembers.size());
            sbState.append("\n" + cpuUtilization);
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
                record.setVersion(++version);
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
                logger.log(Level.FINEST, "Migration failed " + rec.getKeyData());
            }
        }
    }

    public boolean isOwned(Record record) {
        Block block = partitionManager.getOrCreateBlock(record.getBlockId());
        return thisAddress.equals(block.getOwner());
    }

    public int getPartitionCount() {
        return PARTITION_COUNT;
    }

    public Block[] getBlocks() {
        return blocks;
    }

    public Map<String, CMap> getCMaps() {
        return maps;
    }

    Object tryLockAndGet(String name, Object key, long timeout) throws TimeoutException {
        MLock mlock = new MLock();
        boolean locked = mlock.lockAndGetValue(name, key, timeout);
        if (!locked) {
            throw new TimeoutException();
        } else return toObject(mlock.oldValue);
    }

    void putAndUnlock(String name, Object key, Object value) {
        MPut mput = ThreadContext.get().getCallCache(node.factory).getMPut();
        mput.txnalPut(CONCURRENT_MAP_PUT_AND_UNLOCK, name, key, value, -1, -1);
    }

    class MLock extends MBackupAndMigrationAwareOp {
        volatile Data oldValue = null;

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
            return lockAndGetValue(name, key, null, timeout);
        }

        public boolean lockAndGetValue(String name, Object key, Object value, long timeout) {
            boolean locked = booleanCall(CONCURRENT_MAP_TRY_LOCK_AND_GET, name, key, value, timeout, -1);
            if (locked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
            }
            return locked;
        }

        @Override
        public void afterGettingResult(Request request) {
            if (request.operation == CONCURRENT_MAP_TRY_LOCK_AND_GET) {
                if (oldValue == null) {
                    oldValue = request.value;
                }
            }
            super.afterGettingResult(request);
        }

        @Override
        public void handleNoneRedoResponse(Packet packet) {
            if (request.operation == CONCURRENT_MAP_TRY_LOCK_AND_GET) {
                oldValue = packet.getValueData();
                request.value = packet.getValueData();
            }
            super.handleNoneRedoResponse(packet);
        }
    }

    class MContainsKey extends MTargetAwareOp {
        Object keyObject = null;
        NearCache nearCache = null;

        public boolean containsEntry(String name, Object key, Object value) {
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, key, value, 0, -1);
        }

        public boolean containsKey(String name, Object key) {
            this.keyObject = key;
            this.nearCache = mapCaches.get(name);
            Data dataKey = toData(key);
            if (nearCache != null) {
                if (nearCache.containsKey(key)) {
                    return true;
                } else if (nearCache.getMaxSize() == Integer.MAX_VALUE) {
                    return false;
                }
            }
            final CMap cMap = maps.get(name);
            if (cMap != null) {
                Record record = cMap.getOwnedRecord(dataKey);
                if (record != null && record.isActive() && record.isValid() && record.getValueData() != null) {
                    if (cMap.readBackupData) {
                        return true;
                    } else {
                        PartitionServiceImpl.PartitionProxy partition = partitionManager.partitionServiceImpl.getPartition(record.getBlockId());
                        if (partition != null && !partition.isMigrating() && partition.getOwner() != null && partition.getOwner().localMember()) {
                            return true;
                        }
                    }
                }
            }
            return booleanCall(CONCURRENT_MAP_CONTAINS, name, dataKey, null, 0, -1);
        }

        @Override
        public void reset() {
            keyObject = null;
            nearCache = null;
            super.reset();
        }

        @Override
        protected void setResult(Object obj) {
            if (obj != null && obj == Boolean.TRUE) {
                if (nearCache != null) {
                    nearCache.setContainsKey(keyObject, request.key);
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

        @Override
        public final void handleNoneRedoResponse(Packet packet) {
            NearCache nearCache = mapCaches.get(request.name);
            if (nearCache != null) {
                nearCache.invalidate(request.key);
            }
            super.handleNoneRedoResponse(packet);
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
            target = getKeyOwner(request);
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
                mapEntry.setHazelcastInstance(node.factory);
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

    void putTransient(String name, Object key, Object value, long timeout, long ttl) {
        MPut mput = new MPut();
        mput.putTransient(name, key, value, timeout, ttl);
    }

    void putTransient(Request request) {
        MPut mput = new MPut();
        mput.request.key = request.key;
        mput.request.value = request.value;
        mput.request.timeout = 0;
        mput.request.ttl = -1;
        mput.request.indexes = request.indexes;
        mput.request.indexTypes = request.indexTypes;
        mput.request.local = true;
        mput.request.setFromRequest(request);
        mput.request.operation = CONCURRENT_MAP_PUT_TRANSIENT;
        mput.request.longValue = (request.value == null) ? Integer.MIN_VALUE : request.value.hashCode();
        request.setBooleanRequest();
        mput.doOp();
        mput.getResultAsBoolean();
        mput.backup(CONCURRENT_MAP_BACKUP_PUT);
    }

    Map getAll(String name, Set keys) {
        Pairs results = getAllPairs(name, keys);
        List<KeyValue> lsKeyValues = results.getKeyValues();
        Map map = new HashMap(lsKeyValues.size());
        for (KeyValue keyValue : lsKeyValues) {
            map.put(toObject(keyValue.getKeyData()), toObject(keyValue.getValueData()));
        }
        return map;
    }

    Pairs getAllPairs(String name, Set keys) {
        while (true) {
            node.checkNodeState();
            try {
                return doGetAll(name, keys);
            } catch (Throwable e) {
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    Pairs doGetAll(String name, Set keys) throws ExecutionException, InterruptedException {
        Pairs results = new Pairs(keys.size());
        final Map<Member, Keys> targetMembers = new HashMap<Member, Keys>(10);
        PartitionServiceImpl partitionService = partitionManager.partitionServiceImpl;
        for (Object key : keys) {
            Data dKey = toData(key);
            Member owner = partitionService.getPartition(dKey).getOwner();
            if (owner == null) {
                owner = thisMember;
            }
            Keys targetKeys = targetMembers.get(owner);
            if (targetKeys == null) {
                targetKeys = new Keys();
                targetMembers.put(owner, targetKeys);
            }
            targetKeys.add(dKey);
        }
        List<Future<Pairs>> lsFutures = new ArrayList<Future<Pairs>>(targetMembers.size());
        for (Member member : targetMembers.keySet()) {
            Keys targetKeys = targetMembers.get(member);
            GetAllCallable callable = new GetAllCallable(name, targetKeys);
            DistributedTask<Pairs> dt = new DistributedTask<Pairs>(callable, member);
            lsFutures.add(dt);
            node.factory.getExecutorService().execute(dt);
        }
        for (Future<Pairs> future : lsFutures) {
            Pairs pairs = future.get();
            if (pairs != null && pairs.getKeyValues() != null) {
                for (KeyValue keyValue : pairs.getKeyValues()) {
                    results.addKeyValue(keyValue);
                }
            }
        }
        return results;
    }

    void doPutAll(String name, Map entries) {
        Pairs pairs = new Pairs(entries.size());
        for (Object key : entries.keySet()) {
            Object value = entries.get(key);
            pairs.addKeyValue(new KeyValue(toData(key), toData(value)));
        }
        while (true) {
            node.checkNodeState();
            try {
                doPutAll(name, pairs);
                return;
            } catch (Throwable e) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                }
            }
        }
    }

    void doPutAll(String name, Pairs pairs) throws ExecutionException, InterruptedException {
        final Map<Member, Pairs> targetMembers = new HashMap<Member, Pairs>(10);
        PartitionServiceImpl partitionService = partitionManager.partitionServiceImpl;
        for (KeyValue keyValue : pairs.getKeyValues()) {
            Member owner = partitionService.getPartition(keyValue.getKeyData()).getOwner();
            if (owner == null) {
                owner = thisMember;
            }
            Pairs targetPairs = targetMembers.get(owner);
            if (targetPairs == null) {
                targetPairs = new Pairs();
                targetMembers.put(owner, targetPairs);
            }
            targetPairs.addKeyValue(keyValue);
        }
        List<Future<Boolean>> lsFutures = new ArrayList<Future<Boolean>>(targetMembers.size());
        for (Member member : targetMembers.keySet()) {
            Pairs targetPairs = targetMembers.get(member);
            if (targetPairs != null && targetMembers.size() > 0) {
                PutAllCallable callable = new PutAllCallable(name, targetPairs);
                DistributedTask<Boolean> dt = new DistributedTask<Boolean>(callable, member);
                lsFutures.add(dt);
                node.factory.getExecutorService("hz:putAll").execute(dt);
            }
        }
        for (Future<Boolean> future : lsFutures) {
            future.get();
        }
    }

    public static class PutAllCallable implements Callable<Boolean>, HazelcastInstanceAware, DataSerializable {

        private String mapName;
        private Pairs pairs;
        private FactoryImpl factory = null;

        public PutAllCallable() {
        }

        public PutAllCallable(String mapName, Pairs pairs) {
            this.mapName = mapName;
            this.pairs = pairs;
        }

        public Boolean call() throws Exception {
            final ConcurrentMapManager c = factory.node.concurrentMapManager;
            try {
                CMap cmap = c.getMap(mapName);
                if (cmap == null) {
                    c.enqueueAndWait(new Processable() {
                        public void process() {
                            c.getOrCreateMap(mapName);
                        }
                    }, 100);
                    cmap = c.getMap(mapName);
                }
                if (cmap != null) {
                    for (KeyValue keyValue : pairs.getKeyValues()) {
                        Object value = (cmap.getMapIndexService().hasIndexedAttributes()) ?
                                keyValue.getValue() : keyValue.getValueData();
                        IMap<Object, Object> map = (IMap) factory.getOrCreateProxyByName(cmap.name);
                        map.put(keyValue.getKeyData(), value);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            return Boolean.TRUE;
        }

        public void readData(DataInput in) throws IOException {
            mapName = in.readUTF();
            pairs = new Pairs();
            pairs.readData(in);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(mapName);
            pairs.writeData(out);
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }
    }

    public static class GetAllCallable implements Callable<Pairs>, HazelcastInstanceAware, DataSerializable {

        private String mapName;
        private Keys keys;
        private FactoryImpl factory = null;

        public GetAllCallable() {
        }

        public GetAllCallable(String mapName, Keys keys) {
            this.mapName = mapName;
            this.keys = keys;
        }

        public Pairs call() throws Exception {
            final ConcurrentMapManager c = factory.node.concurrentMapManager;
            Pairs pairs = new Pairs();
            try {
                CMap cmap = c.getMap(mapName);
                if (cmap == null) {
                    c.enqueueAndWait(new Processable() {
                        public void process() {
                            c.getOrCreateMap(mapName);
                        }
                    }, 100);
                    cmap = c.getMap(mapName);
                }
                if (cmap != null) {
                    Collection<Object> keysToLoad = (cmap.loader != null) ? new HashSet<Object>() : null;
                    Set<Data> missingKeys = new HashSet<Data>(1);
                    for (Data key : keys.getKeys()) {
                        boolean exist = false;
                        Record record = cmap.getRecord(key);
                        if (record != null && record.isActive() && record.isValid()) {
                            Data value = record.getValueData();
                            if (value != null) {
                                pairs.addKeyValue(new KeyValue(key, value));
                                record.setLastAccessed();
                                exist = true;
                            }
                        }
                        if (!exist) {
                            missingKeys.add(key);
                            if (keysToLoad != null) {
                                keysToLoad.add(toObject(key));
                            }
                        }
                    }
                    if (keysToLoad != null && keysToLoad.size() > 0 && cmap.loader != null) {
                        final Map<Object, Object> mapLoadedEntries = cmap.loader.loadAll(keysToLoad);
                        if (mapLoadedEntries != null) {
                            for (Object key : mapLoadedEntries.keySet()) {
                                Data dKey = toData(key);
                                Object value = mapLoadedEntries.get(key);
                                Data dValue = toData(value);
                                if (dKey != null && dValue != null) {
                                    pairs.addKeyValue(new KeyValue(dKey, dValue));
                                    c.putTransient(mapName, key, value, 0, -1);
                                } else {
                                    missingKeys.add(dKey);
                                }
                            }
                        }
                    }
                    if (cmap.loader == null && !missingKeys.isEmpty()) {
                        ThreadContext threadContext = ThreadContext.get();
                        CallContext realCallContext = threadContext.getCallContext();
                        try {
                            threadContext.setCallContext(CallContext.DUMMY_CLIENT);
                            MProxy mproxy = (MProxy) factory.getOrCreateProxyByName(mapName);
                            for (Data key : missingKeys) {
                                Data value = (Data) mproxy.get(key);
                                if (value != null) {
                                    pairs.addKeyValue(new KeyValue(key, value));
                                }
                            }
                        } finally {
                            threadContext.setCallContext(realCallContext);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
            return pairs;
        }

        public void readData(DataInput in) throws IOException {
            mapName = in.readUTF();
            keys = new Keys();
            keys.readData(in);
        }

        public void writeData(DataOutput out) throws IOException {
            out.writeUTF(mapName);
            keys.writeData(out);
        }

        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.factory = (FactoryImpl) hazelcastInstance;
        }
    }

    class MGet extends MTargetAwareOp {
        Object keyObject = null;

        public Object get(String name, Object key, long timeout) {
            this.keyObject = key;
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (txn.has(name, key)) {
                    return txn.get(name, key);
                }
            }
            final CMap cMap = maps.get(name);
            if (cMap != null) {
                NearCache nearCache = cMap.nearCache;
                if (nearCache != null) {
                    Object value = nearCache.get(key);
                    if (value != null) {
                        return value;
                    }
                }
                final Data dataKey = toData(key);
                Record ownedRecord = cMap.getOwnedRecord(dataKey);
                if (ownedRecord != null && ownedRecord.isActive() && ownedRecord.isValid()) {
                    long version = ownedRecord.getVersion();
                    Object result = null;
                    if (tc.isClient()) {
                        final Data valueData = ownedRecord.getValueData();
                        if (valueData != null && valueData.size() > 0) {
                            result = valueData;
                        }
                    } else {
                        final Object value = ownedRecord.getValue();
                        if (value != null) {
                            result = value;
                        }
                    }
                    if (result != null && ownedRecord.getVersion() == version) {
                        ownedRecord.setLastAccessed();
                        return result;
                    }
                }
                if (cMap.readBackupData) {
                    final Record record = cMap.mapRecords.get(dataKey);
                    if (record != null && cMap.isBackup(record) &&
                            record.isActive() && record.isValid()) {
                        final Data valueData = record.getValueData();
                        if (valueData != null && valueData.size() > 0) {
                            return tc.isClient() ? valueData : toObject(valueData);
                        }
                    }
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
            keyObject = null;
            super.reset();
        }

        @Override
        public final void handleNoneRedoResponse(Packet packet) {
            final CMap cMap = maps.get(request.name);
            if (cMap != null) {
                NearCache nearCache = cMap.nearCache;
                if (nearCache != null) {
                    Data value = packet.getValueData();
                    if (value != null && value.size() > 0) {
                        nearCache.put(this.keyObject, request.key, packet.getValueData());
                    }
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
                            oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
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

        public Object tryRemove(String name, Object key, long timeout) throws TimeoutException {
            Object result = txnalRemove(CONCURRENT_MAP_REMOVE, name, key, null, timeout);
            if (result != null && result instanceof DistributedTimeoutException) {
                throw new TimeoutException();
            }
            return result;
        }

        private Object txnalRemove(ClusterOperation operation, String name,
                                   Object key, Object value, long timeout) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, timeout);
                    if (!locked) {
                        throwCME(key);
                    }
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
                    }
                    int removedValueCount = 0;
                    if (oldObject != null) {
                        if (oldObject instanceof DistributedTimeoutException) {
                            return oldObject;
                        }
                        if (oldObject instanceof CMap.Values) {
                            CMap.Values values = (CMap.Values) oldObject;
                            removedValueCount = values.size();
                        } else {
                            removedValueCount = 1;
                        }
                    }
                    txn.attachRemoveOp(name, key, value, (oldObject == null), removedValueCount);
                    return oldObject;
                } else {
                    return txn.attachRemoveOp(name, key, value, false);
                }
            } else {
                Object oldValue = objectCall(operation, name, key, value, timeout, -1);
                if (oldValue != null) {
                    if (oldValue instanceof AddressAwareException) {
                        rethrowException(operation, (AddressAwareException) oldValue);
                    }
                    if (!(oldValue instanceof DistributedTimeoutException)) {
                        backup(CONCURRENT_MAP_BACKUP_REMOVE);
                    }
                }
                return oldValue;
            }
        }

        @Override
        public final void handleNoneRedoResponse(Packet packet) {
            NearCache nearCache = mapCaches.get(request.name);
            if (nearCache != null) {
                nearCache.invalidate(request.key);
            }
            super.handleNoneRedoResponse(packet);
        }
    }

    public void destroy(String name) {
        CMap cmap = maps.remove(name);
        if (cmap != null) {
            cmap.reset();
        }
        mapCaches.remove(name);
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
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key, value)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, value, DEFAULT_TXN_TIMEOUT);
                    if (!locked)
                        throwCME(key);
                    boolean added = (mlock.oldValue == null);
                    if (added) {
                        txn.attachPutOp(name, key, value, true);
                    }
                    return added;
                } else {
                    return false;
                }
            } else {
                boolean result = booleanCall(CONCURRENT_MAP_PUT_MULTI, name, key, value, 0, -1);
                if (result) {
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                }
                return result;
            }
        }
    }

    boolean isMapIndexed(String name) {
        CMap cmap = getMap(name);
        return cmap != null && (cmap.getMapIndexService().hasIndexedAttributes());
    }

    void setIndexValues(Request request, Object value) {
        CMap cmap = getMap(request.name);
        if (cmap != null) {
            Long[] indexes = cmap.getMapIndexService().getIndexValues(value);
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

    class MAtomic extends MBackupAndMigrationAwareOp {
        final Data nameAsKey;
        final ClusterOperation op;
        final long expected;
        final long value;
        final boolean ignoreExpected;

        MAtomic(Data nameAsKey, ClusterOperation op, long value, long expected, boolean ignoreExpected) {
            this.nameAsKey = nameAsKey;
            this.op = op;
            this.value = value;
            this.expected = expected;
            this.ignoreExpected = ignoreExpected;
        }

        MAtomic(Data nameAsKey, ClusterOperation op, long value, long expected) {
            this(nameAsKey, op, value, expected, false);
        }

        MAtomic(Data nameAsKey, ClusterOperation op, long value) {
            this(nameAsKey, op, value, 0, true);
        }

        boolean doBooleanAtomic() {
            Data expectedData = (ignoreExpected) ? null : toData(expected);
            setLocal(op, FactoryImpl.ATOMIC_NUMBER_MAP_NAME, nameAsKey, expectedData, 0, 0);
            request.longValue = value;
            request.setBooleanRequest();
            doOp();
            Object returnObject = getResultAsBoolean();
            if (returnObject instanceof AddressAwareException) {
                rethrowException(op, (AddressAwareException) returnObject);
            }
            return !Boolean.FALSE.equals(returnObject);
        }

        long doLongAtomic() {
            setLocal(op, FactoryImpl.ATOMIC_NUMBER_MAP_NAME, nameAsKey, null, 0, 0);
            request.longValue = value;
            doOp();
            Object returnObject = getResultAsObject(false);
            return (Long) returnObject;
        }

        void backup(Long value) {
            request.value = toData(value);
            backup(CONCURRENT_MAP_BACKUP_PUT);
        }
    }

    class MSemaphore extends MBackupAndMigrationAwareOp {
        final Data nameAsKey;
        final ClusterOperation op;
        final int expected;
        final int value;
        final boolean ignoreExpected;

        MSemaphore(Data nameAsKey, ClusterOperation op, int value, int expected, boolean ignoreExpected) {
            this.nameAsKey = nameAsKey;
            this.op = op;
            this.value = value;
            this.expected = expected;
            this.ignoreExpected = ignoreExpected;
        }

        MSemaphore(Data nameAsKey, ClusterOperation op, int value, int expected) {
            this(nameAsKey, op, value, expected, false);
        }

        MSemaphore(Data nameAsKey, ClusterOperation op, int value) {
            this(nameAsKey, op, value, 0, true);
        }

        boolean tryAcquire(int permits, long timeout, TimeUnit timeUnit) {
            Boolean result = false;
            long start = System.currentTimeMillis();
            setLocal(op, FactoryImpl.SEMAPHORE_MAP_NAME, nameAsKey, permits, timeUnit.convert(timeout, TimeUnit.MILLISECONDS), 0);
            request.longValue = value;
            request.caller = thisAddress;
            request.operation = SEMAPHORE_ACQUIRE;
            doOp();
            Integer remaining = (Integer) getResultAsObject(false);
            long end = System.currentTimeMillis();
            //Estimate the invocation time, so that it can be deducted from the timeout.
            long diff = end - start;
            if (remaining > 0) {
                if (timeout > 0 && timeout > diff) {
                    result = tryAcquire(remaining, timeout - diff, timeUnit);
                } else if (timeout < 0) {
                    result = tryAcquire(remaining, timeout, timeUnit);
                } else {
                    result = false;
                }
            } else result = true;
            if (!result) {
                tryRelease(permits - remaining, -1, TimeUnit.MILLISECONDS);
            }
            return result;
        }

        void tryRelease(int permits, long timeout, TimeUnit timeUnit) {
            setLocal(op, FactoryImpl.SEMAPHORE_MAP_NAME, nameAsKey, permits, timeUnit.convert(timeout, TimeUnit.MILLISECONDS), 0);
            request.longValue = value;
            request.caller = thisAddress;
            request.operation = SEMAPHORE_RELEASE;
            doOp();
            Integer result = (Integer) getResultAsObject(false);
        }

        int availablePermits() {
            setLocal(op, FactoryImpl.SEMAPHORE_MAP_NAME, nameAsKey, 0, 0, 0);
            request.longValue = value;
            request.caller = thisAddress;
            request.operation = SEMAPHORE_AVAILABLE_PERIMITS;
            doOp();
            Object returnObject = getResultAsObject(false);
            return (Integer) returnObject;
        }

        void drainPermits() {
            setLocal(op, FactoryImpl.SEMAPHORE_MAP_NAME, nameAsKey, 0, 0, 0);
            request.longValue = value;
            request.caller = thisAddress;
            request.operation = SEMAPHORE_DRAIN_PERIMITS;
            doOp();
        }

        void backup(Long value) {
            request.value = toData(value);
            backup(CONCURRENT_MAP_BACKUP_PUT);
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

        public Object putTransient(String name, Object key, Object value, long timeout, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT_TRANSIENT, name, key, value, timeout, ttl);
        }

        public boolean set(String name, Object key, Object value, long ttl) {
            Object result = txnalPut(CONCURRENT_MAP_SET, name, key, value, -1, ttl);
            return (result == Boolean.TRUE);
        }

        public void merge(Record record) {
            if (getInstanceType(record.getName()).isMultiMap()) {
                Set<Data> values = record.getMultiValues();
                if (values != null && values.size() > 0) {
                    for (Data value : values) {
                        mergeOne(record, value);
                    }
                }
            } else {
                mergeOne(record, record.getValueData());
            }
        }

        public void mergeOne(Record record, Data valueData) {
            DataRecordEntry dataRecordEntry = new DataRecordEntry(record, valueData);
            request.setFromRecord(record);
            request.operation = CONCURRENT_MAP_MERGE;
            request.value = toData(dataRecordEntry);
            request.setBooleanRequest();
            doOp();
            Boolean returnObject = getResultAsBoolean();
            if (returnObject) {
                request.value = valueData;
                backup(CONCURRENT_MAP_BACKUP_PUT);
            }
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
                        oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
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
                if (!Boolean.FALSE.equals(returnObject)) {
                    request.value = dataNew;
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                }
                return returnObject;
            }
        }

        Object txnalPut(ClusterOperation operation, String name, Object key, Object value, long timeout, long ttl) {
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
                        oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
                    }
                    if (operation == ClusterOperation.CONCURRENT_MAP_PUT_IF_ABSENT && oldObject != null) {
                        txn.attachPutOp(name, key, oldObject, 0, ttl, false);
                    } else {
                        txn.attachPutOp(name, key, value, 0, ttl, (oldObject == null));
                    }
                    return oldObject;
                } else {
                    if (operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                        Object existingValue = txn.get(name, key);
                        if (existingValue != null) {
                            return existingValue;
                        }
                    }
                    return txn.attachPutOp(name, key, value, false);
                }
            } else {
                setLocal(operation, name, key, value, timeout, ttl);
                request.longValue = (request.value == null) ? Integer.MIN_VALUE : request.value.hashCode();
                setIndexValues(request, value);
                if (operation == CONCURRENT_MAP_TRY_PUT
                        || operation == CONCURRENT_MAP_SET
                        || operation == CONCURRENT_MAP_PUT_AND_UNLOCK
                        || operation == CONCURRENT_MAP_PUT_TRANSIENT) {
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
                    if (operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL && returnObject == null) {
                        return null;
                    }
                    if (returnObject instanceof AddressAwareException) {
                        rethrowException(operation, (AddressAwareException) returnObject);
                    }
                    request.longValue = Long.MIN_VALUE;
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                    return returnObject;
                }
            }
        }
    }

    class MRemoveMulti extends MBackupAndMigrationAwareOp {

        boolean remove(String name, Object key, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, value, DEFAULT_TXN_TIMEOUT);
                    if (!locked) throwCME(key);
                    Data oldValue = mlock.oldValue;
                    boolean existingRecord = (oldValue != null);
                    txn.attachRemoveOp(name, key, value, !existingRecord);
                    return existingRecord;
                } else {
                    MContainsKey mContainsKey = new MContainsKey();
                    boolean containsEntry = mContainsKey.containsEntry(name, key, value);
                    txn.attachRemoveOp(name, key, value, !containsEntry);
                    return containsEntry;
                }
            } else {
                boolean result = booleanCall(CONCURRENT_MAP_REMOVE_MULTI, name, key, value, 0, -1);
                if (result) {
                    backup(CONCURRENT_MAP_BACKUP_REMOVE_MULTI);
                }
                return result;
            }
        }
    }

    abstract class MBackupAndMigrationAwareOp extends MBackupAwareOp {
        @Override
        public boolean isMigrationAware() {
            return true;
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
            if (target == null) {
                target = getKeyOwner(request);
            }
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
            if (!thisAddress.equals(getKeyOwner(request))) {
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
            if (thisAddress.equals(target) &&
                    (operation == CONCURRENT_MAP_LOCK
                            || operation == CONCURRENT_MAP_UNLOCK)) {
                return;
            }
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

    abstract class MigrationAwareSubCall extends SubCall {

        protected MigrationAwareSubCall(Address target) {
            super(target);
        }

        @Override
        public void process() {
            request.blockId = partitionManager.hashBlocks();
            super.process();
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    final void fireMapEvent(Map<Address, Boolean> mapListeners, int eventType,
                            Data oldValue, Record record, Address callerAddress) {
        if (record.getListeners() == null && (mapListeners == null || mapListeners.size() == 0)) {
            return;
        }
        checkServiceThread();
        fireMapEvent(mapListeners, record.getName(), eventType, record.getKeyData(),
                oldValue, record.getValueData(), record.getListeners(), callerAddress);
    }

    public class MContainsValue extends MultiCall<Boolean> {
        boolean contains = false;
        final String name;
        final Object value;

        public MContainsValue(String name, Object value) {
            this.name = name;
            this.value = value;
        }

        SubCall createNewTargetAwareOp(Address target) {
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

        class MGetContainsValue extends MigrationAwareSubCall {
            public MGetContainsValue(Address target) {
                super(target);
                setLocal(CONCURRENT_MAP_CONTAINS_VALUE, name, null, value, 0, -1);
                request.setBooleanRequest();
            }
        }
    }

    public class MLockMap extends MultiCall<Boolean> {
        private final String name;
        private final ClusterOperation operation;
        private volatile boolean result;

        public MLockMap(String name, boolean lock) {
            this.name = name;
            this.operation = (lock) ? CONCURRENT_MAP_LOCK_MAP : CONCURRENT_MAP_UNLOCK_MAP;
        }

        SubCall createNewTargetAwareOp(Address target) {
            return new MTargetLockMap(target);
        }

        boolean onResponse(Object response) {
            return (Boolean.TRUE.equals(response));
        }

        void onCall() {
        }

        @Override
        void onComplete() {
            this.result = true;
        }

        Boolean returnResult() {
            return result;
        }

        @Override
        protected Address getFirstAddressToMakeCall() {
            return node.getMasterAddress();
        }

        class MTargetLockMap extends SubCall {
            public MTargetLockMap(Address target) {
                super(target);
                setLocal(operation, name, null, null, 0, -1);
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

        SubCall createNewTargetAwareOp(Address target) {
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

        class MGetSize extends MigrationAwareSubCall {
            public MGetSize(Address target) {
                super(target);
                setLocal(CONCURRENT_MAP_SIZE, name);
                request.setLongRequest();
            }
        }
    }

    public class MEmpty {

        public boolean isEmpty(String name) {
            NearCache nearCache = mapCaches.get(name);
            if (nearCache != null && !nearCache.isEmpty()) {
                return false;
            }
            final CMap cMap = maps.get(name);
            if (cMap != null) {
                long now = System.currentTimeMillis();
                for (Record record : cMap.mapRecords.values()) {
                    if (record.isActive() && record.isValid(now) && record.getValueData() != null) {
                        if (cMap.readBackupData) {
                            return false;
                        } else {
                            Partition partition = partitionManager.partitionServiceImpl.getPartition(record.getBlockId());
                            if (partition != null && partition.getOwner() != null && partition.getOwner().localMember()) {
                                return false;
                            }
                        }
                    }
                }
            }
            return new MSize(name).getSize() == 0;
        }
    }

    public LocalMapStatsImpl getLocalMapStats(String name) {
        final CMap cmap = getMap(name);
        if (cmap == null) {
            return new LocalMapStatsImpl();
        }
        return cmap.getLocalMapStats();
    }

    public class MIterateLocal extends MGetEntries {

        private final String name;
        private final Predicate predicate;

        public MIterateLocal(String name, Predicate predicate) {
            super(thisAddress, CONCURRENT_MAP_ITERATE_KEYS, name, predicate);
            this.name = name;
            this.predicate = predicate;
            doOp();
        }

        public Set iterate() {
            Entries entries = new Entries(name, CONCURRENT_MAP_ITERATE_KEYS, predicate);
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

        SubCall createNewTargetAwareOp(Address target) {
            return new MGetEntries(target, operation, name, predicate);
        }

        void onCall() {
            entries = new Entries(name, operation, predicate);
        }

        boolean onResponse(Object response) {
            // If Caller Thread is client, then the response is in
            // the form of Data so We need to deserialize it here
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

    class MGetEntries extends MigrationAwareSubCall {
        public MGetEntries(Address target, ClusterOperation operation, String name, Predicate predicate) {
            super(target);
            setLocal(operation, name, null, predicate, -1, -1);
        }
    }

    public Address getKeyOwner(Request req) {
        return getKeyOwner(req.key);
    }

    private Address getKeyOwner(int blockId) {
        checkServiceThread();
        Block block = blocks[blockId];
        if (block == null) {
            if (isMaster() && !isSuperClient()) {
                block = partitionManager.getOrCreateBlock(blockId);
                block.setOwner(thisAddress);
                block.setMigrationAddress(null);
                partitionManager.lsBlocksToMigrate.clear();
                partitionManager.invalidateBlocksHash();
            } else {
                return null;
            }
        } else if (block.getOwner() == null && isMaster() && !isSuperClient()) {
            block.setOwner(thisAddress);
            block.setMigrationAddress(null);
            partitionManager.lsBlocksToMigrate.clear();
            partitionManager.invalidateBlocksHash();
        }
        if (block.isMigrating()) {
            return null;
        }
        return block.getOwner();
    }

    public Address getKeyOwner(Data key) {
        int blockId = getBlockId(key);
        return getKeyOwner(blockId);
    }

    @Override
    public boolean isMigrating(Request req) {
        return partitionManager.isMigrating(req);
    }

    public int getBlockId(Request req) {
        if (req.blockId == -1) {
            req.blockId = getBlockId(req.key);
        }
        return req.blockId;
    }

    public int getBlockId(Data key) {
        int hash = key.hashCode();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % PARTITION_COUNT;
    }

    public long newRecordId() {
        return newRecordId++;
    }

    Block getOrCreateBlock(Request req) {
        return partitionManager.getOrCreateBlock(getBlockId(req));
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

    @Override
    void handleListenerRegistrations(boolean add, String name, Data key, Address address, boolean includeValue) {
        CMap cmap = getOrCreateMap(name);
        if (add) {
            cmap.addListener(key, address, includeValue);
        } else {
            cmap.removeListener(key, address);
        }
    }
    // isMaster should call this method

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
            Blocks blocks = (Blocks) toObject(packet.getValueData());
            partitionManager.handleBlocks(blocks);
            releasePacket(packet);
        }
    }

    class BlockInfoOperationHandler implements PacketProcessor {

        public void process(Packet packet) {
            Block blockInfo = (Block) toObject(packet.getValueData());
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
                NearCache nearCache = cmap.nearCache;
                if (nearCache != null) {
                    nearCache.invalidate(packet.getKeyData());
                }
            }
            releasePacket(packet);
        }
    }

    abstract class MTargetAwareOperationHandler extends TargetAwareOperationHandler {
        boolean isRightRemoteTarget(Request request) {
            boolean callerKnownMember = (request.local || getMember(request.caller) != null);
            return callerKnownMember && thisAddress.equals(getKeyOwner(request));
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

    class PutTransientOperationHandler extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = ensureRecord(request);
            boolean dirty = (record == null) ? false : record.isDirty();
            cmap.put(request);
            if (record != null) {
                record.setDirty(dirty);
            }
            request.value = null;
            request.response = Boolean.TRUE;
        }
    }

    class PutOperationHandler extends StoreAwareOperationHandler {
        @Override
        protected void onNoTimeToSchedule(Request request) {
            request.response = null;
            if (request.operation == CONCURRENT_MAP_TRY_PUT
                    || request.operation == CONCURRENT_MAP_PUT_AND_UNLOCK) {
                request.response = Boolean.FALSE;
            }
            returnResponse(request);
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.put(request);
            if (request.operation == CONCURRENT_MAP_TRY_PUT
                    || request.operation == CONCURRENT_MAP_PUT_AND_UNLOCK) {
                request.response = Boolean.TRUE;
            }
        }
    }

    class AtomicOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.doAtomic(request);
        }
    }

    class SemaphoreOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.doSemaphore(request);
        }
    }

    class AddOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.add(request, false);
        }
    }

    class GetMapEntryOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.getMapEntry(request);
        }
    }

    class EvictOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request);
                if (record != null && record.isActive() && cmap.loader != null &&
                        cmap.writeDelayMillis > 0 && record.isValid() && record.isDirty()) {
                    // if the map has write-behind and the record is dirty then
                    // we have to make sure that the entry is actually persisted
                    // before we can evict it.
                    record.setDirty(false);
                    request.value = record.getValueData();
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
                Record record = cmap.getRecord(request);
                cmap.markAsDirty(record);
                request.response = Boolean.FALSE;
            }
            returnResponse(request);
        }
    }

    class MergeOperationHandler extends StoreAwareOperationHandler {

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isMultiMap()) {
                cmap.putMulti(request);
            } else {
                cmap.put(request);
            }
            request.response = Boolean.TRUE;
        }

        protected boolean shouldExecuteAsync(Request request) {
            return true;
        }

        public void afterExecute(Request request) {
            if (request.response != Boolean.FALSE) {
                doOperation(request);
            }
            returnResponse(request);
        }
    }

    class GetOperationHandler extends StoreAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request);
                if (cmap.loader != null
                        && (record == null
                        || !record.isActive()
                        || !record.isValid()
                        || record.getValueData() == null)) {
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
                    if (record.getValueData() == null) {
                        record.setValue(request.value);
                    }
                    CMap cmap = getOrCreateMap(request.name);
                    record.setIndexes(request.indexes, request.indexTypes);
                    cmap.markAsActive(record);
                    cmap.updateIndexes(record);
                    request.response = record.getValueData();
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
            if (request.key != null) {
                boolean callerKnownMember = (request.local || getMember(request.caller) != null);
                if (!callerKnownMember) {
                    request.response = OBJECT_REDO;
                    return;
                }
            }
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
                    if (record.getLockCount() == 0 &&
                            record.valueCount() == 0 &&
                            !record.hasScheduledAction()) {
                        cmap.markAsEvicted(record);
                    }
                    cmap.fireScheduledActions(record);
                }
                logger.log(Level.FINEST, unlocked + " [" + record.getName() + "] now lock " + record.getLock());
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

    void scheduleRequest(final SchedulableOperationHandler handler, final Request request) {
        final Record record = ensureRecord(request);
        request.scheduled = true;
        ScheduledAction scheduledAction = new ScheduledAction(request) {
            @Override
            public boolean consume() {
                handler.handle(request);
                return true;
            }

            @Override
            public void onExpire() {
                handler.onNoTimeToSchedule(request);
            }

            @Override
            public void onMigrate() {
                request.response = OBJECT_REDO;
                returnResponse(request);
            }
        };
        record.addScheduledAction(scheduledAction);
        node.clusterManager.registerScheduledAction(scheduledAction);
    }

    final DistributedTimeoutException distributedTimeoutException = new DistributedTimeoutException();
    final Data dataTimeoutException = toData(distributedTimeoutException);

    abstract class SchedulableOperationHandler extends MTargetAwareOperationHandler {

        protected boolean shouldSchedule(Request request) {
            return (!testLock(request));
        }

        protected void onNoTimeToSchedule(Request request) {
            if (request.operation == CONCURRENT_MAP_REMOVE) {
                if (request.local) {
                    request.response = distributedTimeoutException;
                } else {
                    request.response = dataTimeoutException;
                }
            } else {
                request.response = null;
            }
            returnResponse(request);
        }

        protected void schedule(Request request) {
            scheduleRequest(SchedulableOperationHandler.this, request);
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
        OrderedExecutionTask orderedExecutionTask = orderedExecutionTasks[getBlockId(request)];
        int size = orderedExecutionTask.offer(request);
        if (size == 1) {
            storeExecutor.execute(orderedExecutionTask);
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
                    setIndexValues(request, value);
                    request.value = toData(value);
                }
                putTransient(request);
            } else if (request.operation == CONCURRENT_MAP_PUT || request.operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                //store the entry
                Object value = toObject(request.value);
                cmap.store.store(toObject(request.key), value);
                Record storedRecord = cmap.getRecord(request);
                if (storedRecord != null) {
                    storedRecord.setLastStoredTime(System.currentTimeMillis());
                }
            } else if (request.operation == CONCURRENT_MAP_REMOVE) {
                Object key = toObject(request.key);
                if (cmap.loader != null && request.value == null) {
                    Object removedObject = cmap.loader.load(key);
                    if (removedObject == null) {
                        return;
                    } else {
                        request.response = toData(removedObject);
                    }
                }
                // remove the entry
                cmap.store.delete(key);
            } else if (request.operation == CONCURRENT_MAP_EVICT) {
                //store the entry
                cmap.store.store(toObject(request.key), toObject(request.value));
                Record storedRecord = cmap.getRecord(request);
                storedRecord.setLastStoredTime(System.currentTimeMillis());
                request.response = Boolean.TRUE;
            } else if (request.operation == CONCURRENT_MAP_MERGE) {
                boolean success = false;
                Object winner = null;
                if (cmap.mergePolicy != null) {
                    Record existingRecord = cmap.getRecord(request);
                    DataRecordEntry existing = (existingRecord == null) ? null : new DataRecordEntry(existingRecord);
                    DataRecordEntry newEntry = (DataRecordEntry) toObject(request.value);
                    Object key = newEntry.getKey();
                    if (key != null && newEntry.hasValue()) {
                        winner = cmap.mergePolicy.merge(cmap.getName(), newEntry, existing);
                        if (winner != null) {
                            success = true;
                            if (cmap.writeDelayMillis == 0 && cmap.store != null) {
                                Object winnerObject = (winner instanceof Data) ? toObject((Data) winner) : winner;
                                cmap.store.store(key, winnerObject);
                                existingRecord.setLastStoredTime(System.currentTimeMillis());
                                success = (request.response == null);
                            }
                        }
                    }
                }
                if (success) {
                    request.value = toData(winner);
                    request.response = Boolean.TRUE;
                } else {
                    request.value = null;
                    request.response = Boolean.FALSE;
                }
            }
        }

        protected boolean shouldExecuteAsync(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (request.operation == CONCURRENT_MAP_GET) {
                return cmap.loader != null;
            } else return cmap.writeDelayMillis == 0 && cmap.store != null;
        }

        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            boolean checkCapacity = (request.operation == CONCURRENT_MAP_PUT
                    || request.operation == CONCURRENT_MAP_TRY_PUT
                    || request.operation == CONCURRENT_MAP_PUT_AND_UNLOCK);
            boolean overCapacity = checkCapacity && cmap.overCapacity(request);
            if (cmap.isNotLocked(request) && !overCapacity) {
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
                Data response = (Data) request.response;
                if (request.operation == CONCURRENT_MAP_REMOVE) {
                    doOperation(request);
                }
                request.value = response;
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

    class SizeOperationHandler extends ExecutedOperationHandler {
        @Override
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request) && !isMigrating(request)) {
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
                            int myPartitionHashNow = partitionManager.hashBlocks();
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
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request) && !isMigrating(request)) {
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
                    QueryContext queryContext = new QueryContext(cmap.getName(), predicate, cmap.getMapIndexService());
                    Set<MapEntry> results = cmap.getMapIndexService().doQuery(queryContext);
                    boolean evaluateValues = (predicate != null && !queryContext.isStrong());
                    createResultPairs(request, results, evaluateValues, predicate);
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            int callerPartitionHash = request.blockId;
                            if (partitionManager.containsMigratingBlock() || callerPartitionHash != partitionManager.hashBlocks()) {
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
                        if (record.getKeyData() == null || record.getKeyData().size() == 0) {
                            throw new RuntimeException("Key cannot be null or zero-size: " + record.getKeyData());
                        }
                        boolean match = (!evaluateEntries) || predicate.apply(record);
                        if (match) {
                            boolean onlyKeys = (request.operation == CONCURRENT_MAP_ITERATE_KEYS_ALL ||
                                    request.operation == CONCURRENT_MAP_ITERATE_KEYS);
                            Data key = record.getKeyData();
                            if (record.getValueData() != null) {
                                Data value = (onlyKeys) ? null : record.getValueData();
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
            if (!request.local) {
                request.value = null;
            }
            request.response = (pairs.size() > 0) ? ((request.local) ? pairs : toData(pairs)) : null;
        }
    }

    Record recordExist(Request req) {
        CMap cmap = maps.get(req.name);
        if (cmap == null)
            return null;
        return cmap.getRecord(req);
    }

    Record ensureRecord(Request req) {
        checkServiceThread();
        CMap cmap = getOrCreateMap(req.name);
        Record record = cmap.getRecord(req);
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
                        logger.log(Level.WARNING, "Store thrown exception for " + request.operation, e);
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
        final Predicate predicate;

        public Entries(String name, ClusterOperation operation, Predicate predicate) {
            this.name = name;
            this.operation = operation;
            this.predicate = predicate;
            TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
            this.checkValue = (InstanceType.MAP == getInstanceType(name)) &&
                    (operation == CONCURRENT_MAP_ITERATE_VALUES
                            || operation == CONCURRENT_MAP_ITERATE_ENTRIES);
            if (txn != null) {
                List<Map.Entry> entriesUnderTxn = txn.newEntries(name);
                if (entriesUnderTxn != null) {
                    if (predicate != null) {
                        for (Map.Entry entry : entriesUnderTxn) {
                            if (predicate.apply((MapEntry) entry)) {
                                lsKeyValues.add(entry);
                            }
                        }
                    } else {
                        lsKeyValues.addAll(entriesUnderTxn);
                    }
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
                            lsKeyValues.add(createSimpleMapEntry(node.factory, name, key, value));
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
            boolean calledNext = false;
            boolean hasNext = false;

            public EntryIterator(Iterator<Map.Entry> it) {
                super();
                this.it = it;
            }

            public boolean hasNext() {
                if (calledHasNext && !calledNext) {
                    return hasNext;
                }
                calledNext = false;
                calledHasNext = true;
                hasNext = setHasNext();
                return hasNext;
            }

            public boolean setHasNext() {
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
                calledNext = true;
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
