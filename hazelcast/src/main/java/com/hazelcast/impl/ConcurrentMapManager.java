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

package com.hazelcast.impl;

import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.core.*;
import com.hazelcast.impl.base.*;
import com.hazelcast.impl.concurrentmap.*;
import com.hazelcast.impl.executor.ParallelExecutor;
import com.hazelcast.impl.monitor.AtomicNumberOperationsCounter;
import com.hazelcast.impl.monitor.CountDownLatchOperationsCounter;
import com.hazelcast.impl.monitor.LocalMapStatsImpl;
import com.hazelcast.impl.monitor.SemaphoreOperationsCounter;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.impl.wan.WanMergeListener;
import com.hazelcast.merge.MergePolicy;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.Serializer;
import com.hazelcast.partition.Partition;
import com.hazelcast.query.Index;
import com.hazelcast.query.MapIndexService;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryContext;
import com.hazelcast.util.Clock;
import com.hazelcast.util.DistributedTimeoutException;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;

import static com.hazelcast.core.Instance.InstanceType;
import static com.hazelcast.impl.ClusterOperation.*;
import static com.hazelcast.impl.Constants.RedoType.*;
import static com.hazelcast.impl.TransactionImpl.DEFAULT_TXN_TIMEOUT;
import static com.hazelcast.impl.base.SystemLogService.Level.INFO;
import static com.hazelcast.impl.base.SystemLogService.Level.TRACE;
import static com.hazelcast.nio.IOUtil.toData;
import static com.hazelcast.nio.IOUtil.toObject;
import static com.hazelcast.util.Clock.currentTimeMillis;

public class ConcurrentMapManager extends BaseManager {
    private static final String BATCH_OPS_EXECUTOR_NAME = "hz.batch";

    final int partitionCount;
    final int maxBackupCount;
    final long globalRemoveDelayMillis;
    final boolean backupRedoEnabled;
    final boolean logState;
    long lastLogStateTime = currentTimeMillis();
    final ConcurrentMap<String, CMap> maps;
    final ConcurrentMap<String, NearCache> mapCaches;
    final PartitionServiceImpl partitionServiceImpl;
    final PartitionManager partitionManager;
    long newRecordId = 0;
    final ParallelExecutor storeExecutor;
    final ParallelExecutor evictionExecutor;
    final RecordFactory recordFactory;
    final Collection<WanMergeListener> colWanMergeListeners = new CopyOnWriteArrayList<WanMergeListener>();

    ConcurrentMapManager(final Node node) {
        super(node);
        recordFactory = node.initializer.getRecordFactory();
        storeExecutor = node.executorManager.newParallelExecutor(
                node.groupProperties.EXECUTOR_STORE_THREAD_COUNT.getInteger());
        evictionExecutor = node.executorManager.newParallelExecutor(node.groupProperties.EXECUTOR_STORE_THREAD_COUNT.getInteger());
        partitionCount = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
        maxBackupCount = MapConfig.MAX_BACKUP_COUNT;
        backupRedoEnabled = node.groupProperties.BACKUP_REDO_ENABLED.getBoolean();
        int removeDelaySeconds = node.groupProperties.REMOVE_DELAY_SECONDS.getInteger();
        if (removeDelaySeconds <= 0) {
            logger.log(Level.WARNING, GroupProperties.PROP_REMOVE_DELAY_SECONDS
                    + " must be greater than zero. Setting to 1.");
            removeDelaySeconds = 1;
        }
        globalRemoveDelayMillis = removeDelaySeconds * 1000L;
        logState = node.groupProperties.LOG_STATE.getBoolean();
        maps = new ConcurrentHashMap<String, CMap>(10, 0.75f, 1);
        mapCaches = new ConcurrentHashMap<String, NearCache>(10, 0.75f, 1);
        partitionManager = new PartitionManager(this);
        partitionServiceImpl = new PartitionServiceImpl(this);
        node.executorManager.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
            public void run() {
                startCleanup(true, false);
            }
        }, 1, 1, TimeUnit.SECONDS);
        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEntryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET_DATA_RECORD_ENTRY, new GetDataRecordEntryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ASYNC_MERGE, new AsyncMergePacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_WAN_MERGE, new WanMergePacketProcessor());
        registerPacketProcessor(CONCURRENT_MAP_MERGE, new MergeOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_TRY_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_SET, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_AND_UNLOCK, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_IF_ABSENT, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, new PutOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_TRANSIENT, new PutTransientOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_FROM_LOAD, new PutFromLoadOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_SAME, new ReplaceOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_PUT_MULTI, new PutMultiOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE, new RemoveOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_EVICT, new EvictOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_IF_SAME, new RemoveIfSameOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_ITEM, new RemoveItemOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT_AND_UNLOCK, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_ADD, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE_MULTI, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_BACKUP_LOCK, new BackupOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_LOCK, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_IS_KEY_LOCKED, new IsKeyLockedOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_TRY_LOCK_AND_GET, new LockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_UNLOCK, new UnlockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_FORCE_UNLOCK, new ForceUnlockOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_LOCK_MAP, new LockMapOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_UNLOCK_MAP, new LockMapOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_REMOVE_MULTI, new RemoveMultiOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_LIST, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_SET, new AddOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_KEY, new ContainsKeyOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_ENTRY, new ContainsEntryOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_VALUE, new ContainsValueOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_VALUE_COUNT, new ValueCountOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_INVALIDATE, new InvalidateOperationHandler());
        registerPacketProcessor(CONCURRENT_MAP_CLEAR_QUICK, new ClearQuickOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_ADD_AND_GET, new AtomicNumberAddAndGetOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_COMPARE_AND_SET, new AtomicNumberCompareAndSetOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_ADD, new AtomicNumberGetAndAddOperationHandler());
        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_SET, new AtomicNumberGetAndSetOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_AWAIT, new CountDownLatchAwaitOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_COUNT_DOWN, new CountDownLatchCountDownOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_DESTROY, new CountDownLatchDestroyOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_GET_COUNT, new CountDownLatchGetCountOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_GET_OWNER, new CountDownLatchGetOwnerOperationHandler());
        registerPacketProcessor(COUNT_DOWN_LATCH_SET_COUNT, new CountDownLatchSetCountOperationHandler());
        registerPacketProcessor(SEMAPHORE_ATTACH_DETACH_PERMITS, new SemaphoreAttachDetachOperationHandler());
        registerPacketProcessor(SEMAPHORE_CANCEL_ACQUIRE, new SemaphoreCancelAcquireOperationHandler());
        registerPacketProcessor(SEMAPHORE_DESTROY, new SemaphoreDestroyOperationHandler());
        registerPacketProcessor(SEMAPHORE_DRAIN_PERMITS, new SemaphoreDrainOperationHandler());
        registerPacketProcessor(SEMAPHORE_GET_ATTACHED_PERMITS, new SemaphoreGetAttachedOperationHandler());
        registerPacketProcessor(SEMAPHORE_GET_AVAILABLE_PERMITS, new SemaphoreGetAvailableOperationHandler());
        registerPacketProcessor(SEMAPHORE_REDUCE_PERMITS, new SemaphoreReduceOperationHandler());
        registerPacketProcessor(SEMAPHORE_RELEASE, new SemaphoreReleaseOperationHandler());
        registerPacketProcessor(SEMAPHORE_TRY_ACQUIRE, new SemaphoreTryAcquireOperationHandler());
    }

    public PartitionManager getPartitionManager() {
        return partitionManager;
    }

    public void addWanMergeListener(WanMergeListener listener) {
        colWanMergeListeners.add(listener);
    }

    public void removeWanMergeListener(WanMergeListener listener) {
        colWanMergeListeners.remove(listener);
    }

    public void onRestart() {
        enqueueAndWait(new Processable() {
            public void process() {
                partitionManager.reset();
                for (CMap cmap : maps.values()) {
                    // do not invalidate records,
                    // values will be invalidated after merge
                    cmap.reset(false);
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
                logger.log(Level.FINEST, "Destroying CMap[" + cmap.name + "]");
                flush(cmap.name);
                cmap.destroy();
            } catch (Throwable e) {
                if (node.isActive()) {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                }
            }
        }
        reset();
        partitionManager.shutdown();
    }

    public void flush(String name) {
        CMap cmap = getMap(name);
        if (cmap != null && cmap.store != null && cmap.writeDelayMillis > 0) {
            final Set<Record> dirtyRecords = new HashSet<Record>();
            for (Record record : cmap.mapRecords.values()) {
                if (record.isDirty()) {
                    PartitionInfo partition = partitionManager.getPartition(record.getBlockId());
                    Address owner = partition.getOwner();
                    if (owner != null && thisAddress.equals(owner)) {
                        dirtyRecords.add(record);
                        record.setDirty(false);  // set dirty to false, we will store these soon
                    }
                }
            }
            try {
                cmap.runStoreUpdate(dirtyRecords);
            } catch (Throwable e) {
                for (Record dirtyRecord : dirtyRecords) {
                    dirtyRecord.setDirty(true);
                }
                Util.throwUncheckedException(e);
            }
        }
    }

    public void syncForDead(MemberImpl deadMember) {
        syncForDeadSemaphores(deadMember.getAddress());
        syncForDeadCountDownLatches(deadMember.getAddress());
        partitionManager.syncForDead(deadMember);
    }

    void syncForDeadSemaphores(Address deadAddress) {
        if (deadAddress == null) return;
        CMap cmap = maps.get(MapConfig.SEMAPHORE_MAP_NAME);
        if (cmap != null) {
            for (Record record : cmap.mapRecords.values()) {
                DistributedSemaphore semaphore = (DistributedSemaphore) record.getValue();
                if (semaphore != null && semaphore.onDisconnect(deadAddress)) {
                    record.setValueData(toData(semaphore));
                    record.incrementVersion();
                }
            }
        }
    }

    void syncForDeadCountDownLatches(Address deadAddress) {
        if (deadAddress == null) return;
        final CMap cmap = maps.get(MapConfig.COUNT_DOWN_LATCH_MAP_NAME);
        if (deadAddress != null && cmap != null) {
            for (Record record : cmap.mapRecords.values()) {
                DistributedCountDownLatch cdl = (DistributedCountDownLatch) record.getValue();
                if (cdl != null && cdl.isOwnerOrMemberAddress(deadAddress)) {
                    List<ScheduledAction> scheduledActions = record.getScheduledActions();
                    if (scheduledActions != null) {
                        for (ScheduledAction sa : scheduledActions) {
                            node.clusterManager.deregisterScheduledAction(sa);
                            final Request sr = sa.getRequest();
                            sr.clearForResponse();
                            sr.lockAddress = deadAddress;
                            sr.longValue = CountDownLatchProxy.OWNER_LEFT;
                            returnResponse(sr);
                        }
                        scheduledActions.clear();
                    }
                    cdl.setOwnerLeft();
                }
            }
        }
    }

    public void syncForAdd() {
        partitionManager.syncForAdd();
    }

    void logState() {
        long now = currentTimeMillis();
        if (logState && ((now - lastLogStateTime) > 15000)) {
            StringBuffer sbState = new StringBuffer(thisAddress + " State[" + new Date(now));
            sbState.append("]");
            Collection<Call> calls = mapCalls.values();
            sbState.append("\nCall Count:").append(calls.size());
            sbState.append(partitionManager.toString());
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
            sbState.append("\nCluster Size:").append(lsMembers.size());
            sbState.append("\n").append(cpuUtilization);
            sbState.append("\nUsed Memory:");
            sbState.append((total - free) / 1024 / 1024);
            sbState.append("MB");
            logger.log(Level.INFO, sbState.toString());
            lastLogStateTime = now;
        }
    }

    /**
     * Should be called from ExecutorService threads.
     *
     * @param mergingEntry
     */
    public void mergeWanRecord(DataRecordEntry mergingEntry) {
        String name = mergingEntry.getName();
        DataRecordEntry existingEntry = new MGetDataRecordEntry().get(name, mergingEntry.getKeyData());
        final CMap cmap = node.concurrentMapManager.getMap(name);
        MProxy mproxy = (MProxy) node.factory.getOrCreateProxyByName(name);
        MergePolicy mergePolicy = cmap.wanMergePolicy;
        if (mergePolicy == null) {
            logger.log(Level.SEVERE, "Received wan merge but no merge policy defined!");
        } else {
            Object winner = mergePolicy.merge(cmap.getName(), mergingEntry, existingEntry);
            if (winner != null) {
                if (winner == MergePolicy.REMOVE_EXISTING) {
                    mproxy.removeForSync(mergingEntry.getKey());
                    notifyWanMergeListeners(WanMergeListener.EventType.REMOVED);
                } else {
                    mproxy.putForSync(mergingEntry.getKeyData(), winner);
                    notifyWanMergeListeners(WanMergeListener.EventType.UPDATED);
                }
            } else {
                notifyWanMergeListeners(WanMergeListener.EventType.IGNORED);
            }
        }
    }

    void notifyWanMergeListeners(WanMergeListener.EventType eventType) {
        for (WanMergeListener wanMergeListener : colWanMergeListeners) {
            if (eventType == WanMergeListener.EventType.UPDATED) {
                wanMergeListener.entryUpdated();
            } else if (eventType == WanMergeListener.EventType.REMOVED) {
                wanMergeListener.entryRemoved();
            } else {
                wanMergeListener.entryIgnored();
            }
        }
    }

    public int getPartitionCount() {
        return partitionCount;
    }

    public Map<String, CMap> getCMaps() {
        return maps;
    }

    Object tryLockAndGet(String name, Object key, long timeout) throws TimeoutException {
        try {
            MLock mlock = new MLock();
            boolean locked = mlock.lockAndGetValue(name, key, timeout);
            if (!locked) {
                throw new TimeoutException();
            } else {
                return toObject(mlock.oldValue);
            }
        } catch (OperationTimeoutException e) {
            throw new TimeoutException();
        }
    }

    void putAndUnlock(String name, Object key, Object value) {
        ThreadContext tc = ThreadContext.get();
        Data dataKey = toData(key);
        CMap cmap = getMap(name);
        final LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
        final boolean shouldUnlock = localLock != null
                && localLock.getThreadId() == tc.getThreadId();
        final boolean shouldRemove = shouldUnlock && localLock.getCount() == 1;
        MPut mput = tc.getCallCache(node.factory).getMPut();
        if (shouldRemove) {
            mput.txnalPut(CONCURRENT_MAP_PUT_AND_UNLOCK, name, key, value, -1, -1);
            // remove if current LocalLock is not changed
            cmap.mapLocalLocks.remove(dataKey, localLock);
        } else if (shouldUnlock) {
            mput.txnalPut(CONCURRENT_MAP_PUT, name, key, value, -1, -1);
            localLock.decrementAndGet();
        } else {
            mput.clearRequest();
            final String error = "Current thread is not owner of lock. putAndUnlock could not be completed! " +
                    "Thread-Id: " + tc.getThreadId() + ", LocalLock: " + localLock;
            logger.log(Level.WARNING, error);
            throw new IllegalStateException(error);
        }
        mput.clearRequest();
    }

    public void destroyEndpointThreads(Address endpoint, Set<Integer> threadIds) {
        node.clusterManager.invalidateScheduledActionsFor(endpoint, threadIds);
        for (CMap cmap : maps.values()) {
            for (Record record : cmap.mapRecords.values()) {
                DistributedLock lock = record.getLock();
                if (lock != null && lock.isLocked()) {
                    if (endpoint.equals(record.getLockAddress()) && threadIds.contains(record.getLock().getLockThreadId())) {
                        record.clearLock();
                        cmap.fireScheduledActions(record);
                    }
                }
            }
        }
    }

    public PartitionInfo getPartitionInfo(int partitionId) {
        return partitionManager.getPartition(partitionId);
    }

    public void startCleanup(final boolean now, final boolean force) {
        if (now) {
            for (CMap cMap : maps.values()) {
                cMap.startCleanup(force);
            }
        } else {
            node.executorManager.executeNow(new Runnable() {
                public void run() {
                    for (CMap cMap : maps.values()) {
                        cMap.startCleanup(force);
                    }
                }
            });
        }
    }

    public void executeCleanup(final CMap cmap, final boolean force) {
        node.executorManager.executeNow(new Runnable() {
            public void run() {
                cmap.startCleanup(force);
            }
        });
    }

    public boolean lock(String name, Object key, long timeout) {
        MLock mlock = new MLock();
        final boolean booleanCall = timeout >= 0; // tryLock
        try {
            final boolean locked = mlock.lock(name, key, timeout);
            if (!locked && !booleanCall) {
                throw new OperationTimeoutException(mlock.request.operation.toString(),
                        "Lock request is timed out! t: " + mlock.request.timeout);
            }
            return locked;
        } catch (OperationTimeoutException e) {
            if (!booleanCall) {
                throw e;
            } else {
                return false;
            }
        }
    }

    class MLock extends MBackupAndMigrationAwareOp {
        volatile Data oldValue = null;

        public boolean unlock(String name, Object key, long timeout) {
            Data dataKey = toData(key);
            ThreadContext tc = ThreadContext.get();
            CMap cmap = getMap(name);
            if (cmap == null) return false;
            LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
            if (localLock != null && localLock.getThreadId() == tc.getThreadId()) {
                if (localLock.decrementAndGet() > 0) return true;
                boolean unlocked = booleanCall(CONCURRENT_MAP_UNLOCK, name, dataKey, null, timeout, -1);
                // remove if current LocalLock is not changed
                cmap.mapLocalLocks.remove(dataKey, localLock);
                if (unlocked) {
                    request.lockAddress = null;
                    request.lockCount = 0;
                    backup(CONCURRENT_MAP_BACKUP_LOCK);
                }
                return unlocked;
            }
            return false;
        }

        public boolean forceUnlock(String name, Object key) {
            Data dataKey = toData(key);
            boolean unlocked = booleanCall(CONCURRENT_MAP_FORCE_UNLOCK, name, dataKey, null, 0, -1);
            if (unlocked) {
                backup(CONCURRENT_MAP_BACKUP_LOCK);
                CMap cmap = getMap(name);
                if (cmap != null) {
                    LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
                    cmap.mapLocalLocks.remove(dataKey, localLock);
                }
            }
            return unlocked;
        }

        public boolean lock(String name, Object key, long timeout) {
            return lock(CONCURRENT_MAP_LOCK, name, key, null, timeout);
        }

        public boolean lockAndGetValue(String name, Object key, long timeout) {
            return lock(CONCURRENT_MAP_TRY_LOCK_AND_GET, name, key, null, timeout);
        }

        public boolean lockAndGetValue(String name, Object key, Object value, long timeout) {
            return lock(CONCURRENT_MAP_TRY_LOCK_AND_GET, name, key, value, timeout);
        }

        public boolean lock(ClusterOperation op, String name, Object key, Object value, long timeout) {
            Data dataKey = toData(key);
            ThreadContext tc = ThreadContext.get();
            setLocal(op, name, dataKey, value, timeout, -1);
            request.setLongRequest();
            doOp();
            long result = (Long) getResultAsObject();
            if (result == -1L) {
                return false;
            } else {
                CMap cmap = getMap(name);
                if (result == 0) {
                    cmap.mapLocalLocks.remove(dataKey);
                }
                LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
                if (localLock == null || localLock.getThreadId() != tc.getThreadId()) {
                    localLock = new LocalLock(tc.getThreadId());
                    cmap.mapLocalLocks.put(dataKey, localLock);
                }
                if (localLock.incrementAndGet() == 1) {
                    backup(CONCURRENT_MAP_BACKUP_LOCK);
                }
            }
            return true;
        }

        public boolean isLocked(String name, Object key) {
            Data dataKey = toData(key);
            CMap cmap = getMap(name);
            if (cmap != null) {
                LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
                if (localLock != null && localLock.getCount() > 0) {
                    return true;
                }
            }

            setLocal(CONCURRENT_MAP_IS_KEY_LOCKED, name, dataKey, null, -1, -1);
            request.setBooleanRequest();
            doOp();
            return (Boolean) getResultAsObject();
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

        @Override
        protected final void handleInterruption() {
            logger.log(Level.WARNING, Thread.currentThread().getName() + " is interrupted! " +
                    "Hazelcast intentionally suppresses interruption during lock operations " +
                    "to avoid dead-lock conditions. Operation: " + request.operation);
        }

        @Override
        protected final boolean isInterruptible() {
            return false;
        }

        @Override
        protected final boolean canTimeout() {
            return false;
        }
    }

    class MContainsKey extends MTargetAwareOp {
        Object keyObject = null;
        NearCache nearCache = null;

        public boolean containsEntry(String name, Object key, Object value) {
            return booleanCall(CONCURRENT_MAP_CONTAINS_ENTRY, name, key, value, 0, -1);
        }

        public boolean containsKey(String name, Object key) {
            this.keyObject = key;
            this.nearCache = mapCaches.get(name);
            Data dataKey = toData(key);
            if (nearCache != null) {
                if (nearCache.containsKey(key)) {
                    return true;
                }
            }
            final CMap cMap = maps.get(name);
            if (cMap != null) {
                Record record = cMap.getOwnedRecord(dataKey);
                if (record != null && record.isActive() && record.isValid() && record.hasValueData()) {
                    if (cMap.isReadBackupData()) {
                        return true;
                    } else {
                        PartitionServiceImpl.PartitionProxy partition = partitionServiceImpl.getPartition(record.getBlockId());
                        if (partition != null && !partitionManager.isOwnedPartitionMigrating(partition.getPartitionId())
                                && partition.getOwner() != null && partition.getOwner().localMember()) {
                            return true;
                        }
                    }
                }
            }
            return booleanCall(CONCURRENT_MAP_CONTAINS_KEY, name, dataKey, null, 0, -1);
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

        @Override
        protected boolean isInterruptible() {
            return true;
        }
    }

    class MEvict extends MBackupAndMigrationAwareOp {
        public boolean evict(String name, Object key) {
            try {
                return evict(CONCURRENT_MAP_EVICT, name, key);
            } catch (OperationTimeoutException e) {
                return false;
            }
        }

        private boolean evict(ClusterOperation operation, String name, Object key) {
            Data k = (key instanceof Data) ? (Data) key : toData(key);
            request.setLocal(operation, name, k, null, 0, maxOperationTimeout, -1, thisAddress);
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

        @Override
        protected boolean isInterruptible() {
            return true;
        }
    }

    class MGetDataRecordEntry extends MTargetAwareOp {
        public DataRecordEntry get(String name, Object key) {
            Object result = objectCall(CONCURRENT_MAP_GET_DATA_RECORD_ENTRY, name, key, null, 0, -1);
            if (result instanceof Data) {
                result = toObject((Data) result);
            }
            return (DataRecordEntry) result;
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

    public void putTransient(String name, Object key, Object value, long ttl) {
        MPut mput = new MPut();
        mput.putTransient(name, key, value, ttl);
    }

    public boolean putFromLoad(String name, Object key, Object value) {
        try {
            MPut mput = new MPut();
            return (Boolean) mput.putFromLoad(name, key, value);
        } catch (OperationTimeoutException e) {
            return false;
        }
    }

    // used by GetMapEntryOperationHandler, GetOperationHandler, ContainsKeyOperationHandler
    private void putFromLoad(final Request request) {
        final MPut mput = new MPut();
        try {
            mput.request.setFromRequest(request);
            mput.request.timeout = 0;
            mput.request.ttl = -1;
            mput.request.local = true;
            mput.request.operation = CONCURRENT_MAP_PUT_FROM_LOAD;
            mput.request.longValue = (request.value == null) ? Integer.MIN_VALUE : request.value.hashCode();
            request.setBooleanRequest();
            final Data value = request.value;
            mput.doOp();
            boolean success = mput.getResultAsBoolean();
            if (success) {
                mput.request.value = value;
                mput.backup(CONCURRENT_MAP_BACKUP_PUT);
            }
        } catch (OperationTimeoutException e) {
            logger.log(Level.FINEST, "Put-after-load for Operation[" + request.operation + "] has been timed out!");
        }
    }

    Map getAll(String name, Set keys) {
        Set theKeys = keys;
        Map map = new HashMap(keys.size());
        CMap cmap = getMap(name);
        if (cmap != null && cmap.nearCache != null) {
            theKeys = new HashSet(keys);
            for (Iterator iterator = theKeys.iterator(); iterator.hasNext(); ) {
                Object key = iterator.next();
                Object value = cmap.nearCache.get(key);
                if (value != null) {
                    map.put(key, value);
                    iterator.remove();
                }
            }
        }
        if (theKeys.size() > 1) {
            Pairs results = getAllPairs(name, theKeys);
            final List<KeyValue> lsKeyValues = results.getKeyValues();
            cmap = getMap(name);
            if (lsKeyValues.size() > 0 && cmap != null) {
                final NearCache nearCache = cmap.nearCache;
                if (nearCache != null) {
                    final Map<Data, Object> keyObjects = new HashMap<Data, Object>(lsKeyValues.size());
                    for (KeyValue keyValue : lsKeyValues) {
                        keyObjects.put(keyValue.getKeyData(), keyValue.getKey());
                    }
                    enqueueAndReturn(new Processable() {
                        public void process() {
                            for (KeyValue keyValue : lsKeyValues) {
                                final Object key = keyObjects.get(keyValue.getKeyData());
                                if (key != null) {
                                    nearCache.put(key, keyValue.getKeyData(), keyValue.getValueData());
                                }
                            }
                        }
                    });
                }
            }
            for (KeyValue keyValue : lsKeyValues) {
                map.put(keyValue.getKey(), keyValue.getValue());
            }
        } else if (theKeys.size() == 1) {
            Object key = theKeys.iterator().next();
            Object value = new MGet().get(name, key, -1);
            if (value != null) {
                map.put(key, value);
            }
        }
        return map;
    }

    Pairs getAllPairs(String name, Set keys) {
        while (true) {
            try {
                return doGetAll(name, keys);
            } catch (Throwable e) {
                if (e instanceof MemberLeftException) {
                    try {
                        Thread.sleep(redoWaitMillis);
                    } catch (InterruptedException e1) {
                        handleInterruptedException(true, CONCURRENT_MAP_GET_ALL);
                    }
                } else if (e instanceof InterruptedException) {
                    handleInterruptedException(true, CONCURRENT_MAP_GET_ALL);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    Pairs doGetAll(String name, Set keys) throws ExecutionException, InterruptedException {
        Pairs results = new Pairs(keys.size());
        final Map<Member, Keys> targetMembers = new HashMap<Member, Keys>(10);
        for (Object key : keys) {
            Data dKey = toData(key);
            Member owner = partitionServiceImpl.getPartition(dKey).getOwner();
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
            node.factory.getExecutorService(BATCH_OPS_EXECUTOR_NAME).execute(dt);
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

    int size(String name) {
        while (true) {
            try {
                int size = trySize(name);
                TransactionImpl txn = ThreadContext.get().getCallContext().getTransaction();
                if (txn != null) {
                    size += txn.size(name);
                }
                return size;
            } catch (Throwable e) {
                if (e instanceof MemberLeftException || e instanceof IllegalPartitionState) {
                    try {
                        Thread.sleep(redoWaitMillis);
                    } catch (InterruptedException e1) {
                        handleInterruptedException(true, CONCURRENT_MAP_SIZE);
                    }
                } else if (e instanceof InterruptedException) {
                    handleInterruptedException(true, CONCURRENT_MAP_SIZE);
                } else {
                    Util.throwUncheckedException(e);
                    return -1; // not reachable
                }
            }
        }
    }

    int trySize(String name) throws ExecutionException, InterruptedException {
        int totalSize = 0;
        Set<Member> members = node.getClusterImpl().getMembers();
        List<Future<Integer>> lsFutures = new ArrayList<Future<Integer>>();
        int expectedPartitionVersion = partitionManager.getVersion();
        for (Member member : members) {
            if (!member.isLiteMember()) {
                MapSizeCallable callable = new MapSizeCallable(name, expectedPartitionVersion);
                DistributedTask<Integer> dt = new DistributedTask<Integer>(callable, member);
                lsFutures.add(dt);
                node.factory.getExecutorService(BATCH_OPS_EXECUTOR_NAME).execute(dt);
            }
        }
        for (Future<Integer> future : lsFutures) {
            Integer partialSize = future.get();
            if (partialSize != null) {
                if (partialSize == -1) {
                    throw new IllegalPartitionState("Unexpected partition version!");
                }
                totalSize += partialSize;
            }
        }
        return totalSize;
    }

    Entries query(String name, ClusterOperation operation, Predicate predicate) {
        Data predicateData = toData(predicate);
        while (true) {
            try {
                Entries entries = new Entries(this, name, operation, predicate);
                tryQuery(entries, name, operation, predicateData);
                return entries;
            } catch (Throwable e) {
                if (e instanceof MemberLeftException || e instanceof IllegalPartitionState) {
                    try {
                        Thread.sleep(redoWaitMillis);
                    } catch (InterruptedException e1) {
                        handleInterruptedException(true, operation);
                    }
                } else if (e instanceof InterruptedException) {
                    handleInterruptedException(true, operation);
                } else if (e instanceof ExecutionException) {
                    Throwable cause = e.getCause();
                    if (cause != null && cause instanceof RuntimeException) {
                        throw (RuntimeException) cause;
                    } else {
                        throw new RuntimeException(e);
                    }
                } else {
                    Util.throwUncheckedException(e);
                    return null; // not reachable
                }
            }
        }
    }

    void tryQuery(Entries entries, String name, ClusterOperation operation, Data predicateData) throws ExecutionException, InterruptedException {
        Set<Member> members = node.getClusterImpl().getMembers();
        List<Future<Pairs>> lsFutures = new ArrayList<Future<Pairs>>();
        int expectedPartitionVersion = partitionManager.getVersion();
        for (Member member : members) {
            if (!member.isLiteMember()) {
                Callable callable = new MapQueryCallable(name, operation, predicateData, expectedPartitionVersion);
                DistributedTask<Pairs> dt = new DistributedTask<Pairs>(callable, member);
                lsFutures.add(dt);
                node.factory.getExecutorService(BATCH_OPS_EXECUTOR_NAME).execute(dt);
            }
        }
        for (Future<Pairs> future : lsFutures) {
            Pairs pairs = future.get();
            if (pairs == null) {
                throw new IllegalPartitionState("Unexpected partition version!");
            } else {
                entries.addEntries(pairs);
            }
        }
    }

    Entries queryLocal(String name, ClusterOperation operation, Predicate predicate) {
        Entries entries = new Entries(this, name, operation, predicate);
        CMap cmap = getMap(name);
        if (cmap == null) return entries;
        PartitionManager partitionManager = getPartitionManager();
        while (true) {
            int partitionVersion = partitionManager.getVersion();
            Pairs pairs = queryMap(cmap, operation, predicate);
            if (partitionManager.getVersion() == partitionVersion) {
                entries.addEntries(pairs);
                return entries;
            }
            entries.clearEntries();
        }
    }

    void doPutAll(String name, Map entries) {
        Pairs pairs = new Pairs(entries.size());
        for (Object key : entries.keySet()) {
            Object value = entries.get(key);
            pairs.addKeyValue(new KeyValue(toData(key), toData(value)));
        }
        while (true) {
            try {
                doPutAll(name, pairs);
                return;
            } catch (Exception e) {
                if (e instanceof MemberLeftException) {
                    try {
                        Thread.sleep(redoWaitMillis);
                    } catch (InterruptedException e1) {
                        handleInterruptedException(true, CONCURRENT_MAP_PUT_ALL);
                    }
                } else if (e instanceof InterruptedException) {
                    handleInterruptedException(true, CONCURRENT_MAP_PUT_ALL);
                } else if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    void doPutAll(String name, Pairs pairs) throws ExecutionException, InterruptedException {
        final Map<Member, Pairs> targetMembers = new HashMap<Member, Pairs>(10);
        for (KeyValue keyValue : pairs.getKeyValues()) {
            Member owner = partitionServiceImpl.getPartition(keyValue.getKeyData()).getOwner();
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
                node.factory.getExecutorService(BATCH_OPS_EXECUTOR_NAME).execute(dt);
            }
        }
        for (Future<Boolean> future : lsFutures) {
            future.get();
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
                    Data value = txn.get(name, key);
                    return tc.isClient() ? value : toObject(value);
                } else {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) {
                        throwTxTimeoutException(key);
                    }
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = tc.isClient() ? oldValue : tc.toObject(oldValue);
                        txn.attachPutOp(name, key, oldValue, false);
                    } else {
                        txn.attachPutOp(name, key, null, false);
                    }
                    return oldObject;
                }
            }
            final CMap cMap = maps.get(name);
            Data dataKey = null;
            if (cMap != null) {
                cMap.incrementGetCount();
                NearCache nearCache = cMap.nearCache;
                if (nearCache != null) {
                    Object value = nearCache.get(key);
                    if (value != null) {
                        return value;
                    }
                }
                dataKey = toData(key);
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
                if (cMap.isReadBackupData()) {
                    final Record record = cMap.mapRecords.get(dataKey);
                    if (record != null && record.isActive() && record.isValid()) {
                        final Data valueData = record.getValueData();
                        if (valueData != null && valueData.size() > 0) {
                            return tc.isClient() ? valueData : toObject(valueData);
                        }
                    }
                }
            }
            if (dataKey == null) {
                dataKey = toData(key);
            }
            Object value = objectCall(CONCURRENT_MAP_GET, name, dataKey, null, timeout, -1);
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

        @Override
        protected boolean isInterruptible() {
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

        @Override
        protected boolean isInterruptible() {
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
                        if (!locked) {
                            throwTxTimeoutException(key);
                        }
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
                    logger.log(Level.WARNING, e1.getMessage(), e1);
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
        protected boolean shouldRedoWhenOwnerDies() {
            return true;
        }
    }

    class MRemove extends MBackupAndMigrationAwareOp {

        public Object remove(String name, Object key) {
            return txnalRemove(CONCURRENT_MAP_REMOVE, name, key, null, -1, -1L);
        }

        public boolean removeIfSame(String name, Object key, Object value) {
            return txnalRemove(CONCURRENT_MAP_REMOVE_IF_SAME, name, key, value, -1, -1L) == Boolean.TRUE;
        }

        public Object tryRemove(String name, Object key, long timeout) throws TimeoutException {
            try {
                return txnalRemove(CONCURRENT_MAP_REMOVE, name, key, null, timeout, -1L);
            } catch (OperationTimeoutException e) {
                throw new TimeoutException(e.getMessage());
            }
        }

        public void removeForSync(String name, Object key) {
            txnalRemove(CONCURRENT_MAP_REMOVE, name, key, null, -1, Long.MIN_VALUE);
        }

        private Object txnalRemove(ClusterOperation operation, String name,
                                   Object key, Object value, long timeout, long txnId) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) {
                        throwTxTimeoutException(key);
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
                        if (oldObject instanceof Values) {
                            Values values = (Values) oldObject;
                            removedValueCount = values.size();
                        } else {
                            removedValueCount = 1;
                        }
                    }
                    txn.attachRemoveOp(name, key, toData(value), (oldObject == null), removedValueCount);
                    return oldObject;
                } else {
                    Data oldValue = txn.attachRemoveOp(name, key, toData(value), false);
                    Object oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
                    return oldObject;
                }
            } else {
                setLocal(operation, name, key, value, timeout, -1);
                if (txnId != -1) {
                    request.txnId = txnId;
                }
                if (operation == CONCURRENT_MAP_REMOVE) {
                    request.setObjectRequest();
                    doOp();
                    Object oldValue = getResultAsObject();
                    if (oldValue != null) {
                        if (oldValue instanceof AddressAwareException) {
                            rethrowException(operation, (AddressAwareException) oldValue);
                        }
                        if (!(oldValue instanceof DistributedTimeoutException)) {
                            backup(CONCURRENT_MAP_BACKUP_REMOVE);
                        }
                    }
                    return oldValue;
                } else {
                    request.setBooleanRequest();
                    doOp();
                    boolean success = getResultAsBoolean();
                    if (success) {
                        backup(CONCURRENT_MAP_BACKUP_REMOVE);
                    }
                    return success;
                }
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

        @Override
        protected boolean shouldRedoWhenOwnerDies() {
            return true;
        }
    }

    public void destroy(String name) {
        CMap cmap = maps.remove(name);
        if (cmap != null) {
            cmap.destroy();
        }
        mapCaches.remove(name);
    }

    class MMultiGet extends MTargetAwareOp {

        public Collection get(String name, Object key) {
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.getCallContext().getTransaction();
            Object value = objectCall(CONCURRENT_MAP_GET, name, key, null, 0, -1);
            if (value instanceof AddressAwareException) {
                rethrowException(request.operation, (AddressAwareException) value);
            }
            Collection currentValues = (Collection) value;
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                List allValues = new ArrayList();
                if (currentValues != null) {
                    allValues.addAll(currentValues);
                }
                txn.getMulti(name, key, allValues);
                return allValues;
            } else {
                return currentValues != null ? currentValues : Collections.emptySet();
            }
        }

        @Override
        public boolean isMigrationAware() {
            return true;
        }

        @Override
        protected boolean isInterruptible() {
            return true;
        }
    }

    class MPutMulti extends MBackupAndMigrationAwareOp {

        boolean put(String name, Object key, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lock(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) {
                        throwTxTimeoutException(key);
                    }
                }
                if (txn.has(name, key, value)) {
                    return false;
                }
                txn.attachPutMultiOp(name, key, toData(value));
                return true;
            } else {
                boolean result = booleanCall(CONCURRENT_MAP_PUT_MULTI, name, key, value, -1, -1);
                if (result) {
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                }
                return result;
            }
        }

        @Override
        protected boolean shouldRedoWhenOwnerDies() {
            return true;
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

    class MAtomicNumber extends MDefaultBackupAndMigrationAwareOp {
        AtomicNumberOperationsCounter operationsCounter;

        public long addAndGet(Data name, long delta) {
            return doAtomicOp(ATOMIC_NUMBER_ADD_AND_GET, name, delta, null);
        }

        public boolean compareAndSet(Data name, long expectedValue, long newValue) {
            return doAtomicOp(ATOMIC_NUMBER_COMPARE_AND_SET, name, newValue, toData(expectedValue)) == 1;
        }

        public long getAndAdd(Data name, long delta) {
            return doAtomicOp(ATOMIC_NUMBER_GET_AND_ADD, name, delta, null);
        }

        public long getAndSet(Data name, long newValue) {
            return doAtomicOp(ATOMIC_NUMBER_GET_AND_SET, name, newValue, null);
        }

        public void destroy(Data name) {
            new MRemove().remove(MapConfig.ATOMIC_LONG_MAP_NAME, name);
        }

        void setOperationsCounter(AtomicNumberOperationsCounter operationsCounter) {
            this.operationsCounter = operationsCounter;
        }

        private long doAtomicOp(ClusterOperation op, Data name, long value, Data expected) {
            long begin = currentTimeMillis();
            setLocal(op, MapConfig.ATOMIC_LONG_MAP_NAME, name, expected, -1, 0);
            request.longValue = value;
            doOp();
            Data backup = (Data) getResultAsIs();
            long responseValue = request.longValue;
            if (backup != null) {
                request.value = backup;
                request.longValue = 0L;
                backup(CONCURRENT_MAP_BACKUP_PUT);
                operationsCounter.incrementModified(currentTimeMillis() - begin);
            } else {
                operationsCounter.incrementNonModified(currentTimeMillis() - begin);
            }
            return responseValue;
        }
    }

    class MCountDownLatch extends MDefaultBackupAndMigrationAwareOp {
        CountDownLatchOperationsCounter operationsCounter;
        long begin;

        public boolean await(Data name, long timeout, TimeUnit unit) throws InstanceDestroyedException, MemberLeftException {
            try {
                int awaitResult = doCountDownLatchOp(COUNT_DOWN_LATCH_AWAIT, name, 0, unit.toMillis(timeout));
                switch (awaitResult) {
                    case CountDownLatchProxy.INSTANCE_DESTROYED:
                        throw new InstanceDestroyedException(InstanceType.COUNT_DOWN_LATCH, (String) toObject(name));
                    case CountDownLatchProxy.OWNER_LEFT:
                        Member owner = new MemberImpl(request.lockAddress, thisAddress.equals(request.lockAddress));
                        throw new MemberLeftException(owner);
                    case CountDownLatchProxy.AWAIT_DONE:
                        return true;
                    case CountDownLatchProxy.AWAIT_FAILED:
                    default:
                        return false;
                }
            } finally {
                operationsCounter.incrementAwait(currentTimeMillis() - begin);
            }
        }

        public boolean countDown(Data name) {
            final int threadsReleased = doCountDownLatchOp(COUNT_DOWN_LATCH_COUNT_DOWN, name, 0, -1);
            operationsCounter.incrementCountDown(currentTimeMillis() - begin, threadsReleased);
            return threadsReleased > 0;
        }

        public int getCount(Data name) {
            final int count = doCountDownLatchOp(COUNT_DOWN_LATCH_GET_COUNT, name, 0, -1);
            operationsCounter.incrementOther(currentTimeMillis() - begin);
            return count;
        }

        public Address getOwnerAddress(Data name) {
            begin = currentTimeMillis();
            setLocal(COUNT_DOWN_LATCH_GET_OWNER, MapConfig.COUNT_DOWN_LATCH_MAP_NAME, name, null, -1, -1);
            doOp();
            return (Address) getResultAsObject(false);
        }

        public boolean setCount(Data name, int count, Address ownerAddress) {
            int countSet = doCountDownLatchOp(COUNT_DOWN_LATCH_SET_COUNT, name, count, -1, ownerAddress);
            operationsCounter.incrementOther(currentTimeMillis() - begin);
            return countSet == 1;
        }

        public void destroy(Data name) {
            doCountDownLatchOp(COUNT_DOWN_LATCH_DESTROY, name, 0, -1);
            //new MRemove().remove(MapConfig.COUNT_DOWN_LATCH_MAP_NAME, name, -1);
        }

        void setOperationsCounter(CountDownLatchOperationsCounter operationsCounter) {
            this.operationsCounter = operationsCounter;
        }

        private int doCountDownLatchOp(ClusterOperation op, Data name, int value, long timeout) {
            return doCountDownLatchOp(op, name, value, timeout, thisAddress);
        }

        private int doCountDownLatchOp(ClusterOperation op, Data name, int value, long timeout, Address endPoint) {
            begin = currentTimeMillis();
            setLocal(op, MapConfig.COUNT_DOWN_LATCH_MAP_NAME, name, null, timeout, -1);
            request.longValue = value;
            request.lockAddress = endPoint;
            doOp();
            Data backup = (Data) getResultAsIs();
            int responseValue = (int) request.longValue;
            if (backup != null) {
                request.value = backup;
                request.longValue = 0L;
                backup(CONCURRENT_MAP_BACKUP_PUT);
            }
            return responseValue;
        }

        @Override
        protected boolean isInterruptible() {
            return false;
        }

        @Override
        protected boolean canTimeout() {
            return false;
        }
    }

    class MSemaphore extends MDefaultBackupAndMigrationAwareOp {
        SemaphoreOperationsCounter operationsCounter;
        long begin;

        public void attachDetach(Data name, int permitsDelta) {
            doSemaphoreOp(SEMAPHORE_ATTACH_DETACH_PERMITS, name, permitsDelta, null, -1);
            operationsCounter.incrementNonAcquires(currentTimeMillis() - begin, permitsDelta);
        }

        public boolean cancelAcquire(Data name) {
            setLocal(SEMAPHORE_CANCEL_ACQUIRE, MapConfig.SEMAPHORE_MAP_NAME, name, null, -1, -1);
            doOp();
            getResult();
            return request.longValue == 1;
        }

        public int drainPermits(Data name) {
            int drainedPermits = doSemaphoreOp(SEMAPHORE_DRAIN_PERMITS, name, -1, null, -1);
            operationsCounter.incrementNonAcquires(currentTimeMillis() - begin, 0);
            return drainedPermits;
        }

        public int getAvailable(Data name) {
            int availablePermits = doSemaphoreOp(SEMAPHORE_GET_AVAILABLE_PERMITS, name, -1, null, -1);
            operationsCounter.incrementNonAcquires(currentTimeMillis() - begin, 0);
            return availablePermits;
        }

        public int getAttached(Data name) {
            int attachedPermits = doSemaphoreOp(SEMAPHORE_GET_ATTACHED_PERMITS, name, -1, false, -1);
            operationsCounter.incrementNonAcquires(currentTimeMillis() - begin, 0);
            return attachedPermits;
        }

        public void reduce(Data name, int permits) {
            doSemaphoreOp(SEMAPHORE_REDUCE_PERMITS, name, permits, null, -1);
            operationsCounter.incrementPermitsReduced(currentTimeMillis() - begin, 0);
        }

        public void release(Data name, int permits, Boolean detach) {
            doSemaphoreOp(SEMAPHORE_RELEASE, name, permits, detach, -1);
            operationsCounter.incrementReleases(currentTimeMillis() - begin, permits, detach);
        }

        public boolean tryAcquire(Data name, int permits, boolean attach, long timeout) throws InstanceDestroyedException {
            try {
                int acquireResult = doSemaphoreOp(SEMAPHORE_TRY_ACQUIRE, name, permits, attach, timeout);
                switch (acquireResult) {
                    case SemaphoreProxy.INSTANCE_DESTROYED:
                        operationsCounter.incrementRejectedAcquires(currentTimeMillis() - begin);
                        throw new InstanceDestroyedException(InstanceType.SEMAPHORE, (String) toObject(name));
                    case SemaphoreProxy.ACQUIRED:
                        operationsCounter.incrementAcquires(currentTimeMillis() - begin, permits, attach);
                        return true;
                    case SemaphoreProxy.ACQUIRE_FAILED:
                    default:
                        operationsCounter.incrementRejectedAcquires(currentTimeMillis() - begin);
                        return false;
                }
            } catch (RuntimeInterruptedException e) {
                operationsCounter.incrementRejectedAcquires(currentTimeMillis() - begin);
                throw e;
            }
        }

        public void destroy(Data name) {
            doSemaphoreOp(SEMAPHORE_DESTROY, name, -1, null, -1);
            new MRemove().remove(MapConfig.SEMAPHORE_MAP_NAME, name);
        }

        void setOperationsCounter(SemaphoreOperationsCounter operationsCounter) {
            this.operationsCounter = operationsCounter;
        }

        private int doSemaphoreOp(ClusterOperation op, Data name, long longValue, Object value, long timeout) {
            begin = currentTimeMillis();
            int responseValue = 1;
            if (longValue != 0L) {
                setLocal(op, MapConfig.SEMAPHORE_MAP_NAME, name, value, timeout, -1);
                request.longValue = longValue;
                doOp();
                Data backup = (Data) getResultAsIs();
                responseValue = (int) request.longValue;
                if (backup != null) {
                    request.value = backup;
                    request.longValue = 0L;
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                    operationsCounter.incrementModified(currentTimeMillis() - begin);
                } else {
                    operationsCounter.incrementNonModified(currentTimeMillis() - begin);
                }
            }
            return responseValue;
        }

        @Override
        protected boolean isInterruptible() {
            return false;
        }

        @Override
        protected boolean canTimeout() {
            return false;
        }
    }

    class MPut extends MBackupAndMigrationAwareOp {

        public boolean replace(String name, Object key, Object oldValue, Object newValue) {
            Object result = txnalReplaceIfSame(CONCURRENT_MAP_REPLACE_IF_SAME, name, key, newValue, oldValue);
            return (result == Boolean.TRUE);
        }

        public Object replace(String name, Object key, Object value) {
            return txnalPut(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, name, key, value, -1, -1);
        }

        public Object putIfAbsent(String name, Object key, Object value, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT_IF_ABSENT, name, key, value, -1, ttl);
        }

        public Object put(String name, Object key, Object value, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT, name, key, value, -1, ttl);
        }

        public Object putAfterCommit(String name, Object key, Object value, long ttl, long txnId) {
            Object result = null;
            if (txnId != -1) {
                ThreadContext tc = ThreadContext.get();
                Data dataKey = toData(key);
                CMap cmap = getMap(name);
                final LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
                final boolean shouldUnlock = localLock != null
                        && localLock.getThreadId() == tc.getThreadId();
                final boolean shouldRemove = shouldUnlock && localLock.getCount() == 1;
                if (shouldRemove) {
                    result = txnalPut(CONCURRENT_MAP_PUT_AND_UNLOCK, name, key, value, -1, ttl, txnId);
                    // remove if current LocalLock is not changed
                    cmap.mapLocalLocks.remove(dataKey, localLock);
                } else if (shouldUnlock) {
                    result = txnalPut(CONCURRENT_MAP_PUT, name, key, value, -1, ttl, -1);
                    localLock.decrementAndGet();
                } else {
                    final String error = "Could not commit put operation! Current thread is not owner of " +
                            "transaction lock! Thread-Id: " + tc.getThreadId() + ", LocalLock: " + localLock;
                    logger.log(Level.WARNING, error);
                    throw new IllegalStateException(error);
                }
            }
            return result;
        }

        public Object putForSync(String name, Object key, Object value) {
            Object result = txnalPut(CONCURRENT_MAP_SET, name, key, value, -1, -1, Long.MIN_VALUE);
            return (result == Boolean.TRUE);
        }

        public Object putTransient(String name, Object key, Object value, long ttl) {
            return txnalPut(CONCURRENT_MAP_PUT_TRANSIENT, name, key, value, -1, ttl);
        }

        public Object putFromLoad(String name, Object key, Object value) {
            return txnalPut(CONCURRENT_MAP_PUT_FROM_LOAD, name, key, value, 0, -1);
        }

        public boolean set(String name, Object key, Object value, long ttl) {
            Object result = txnalPut(CONCURRENT_MAP_SET, name, key, value, -1, ttl);
            return (result == Boolean.TRUE);
        }

        public void merge(Record record) {
            if (getInstanceType(record.getName()).isMultiMap()) {
                Collection<ValueHolder> values = record.getMultiValues();
                if (values != null && values.size() > 0) {
                    for (ValueHolder valueHolder : values) {
                        mergeOne(record, valueHolder.getData());
                    }
                }
            } else {
                mergeOne(record, record.getValueData());
            }
        }

        public void mergeOne(Record record, Data valueData) {
            DataRecordEntry dataRecordEntry = new DataRecordEntry(record, valueData, false);
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
            try {
                Object result = txnalPut(CONCURRENT_MAP_TRY_PUT, name, key, value, timeout, ttl);
                return (result == Boolean.TRUE);
            } catch (OperationTimeoutException e) {
                return false;
            }
        }

        private Object txnalReplaceIfSame(ClusterOperation operation, String name, Object key, Object newValue, Object expectedValue) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) {
                        throwTxTimeoutException(key);
                    }
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
                    }
                    if (oldObject == null) {
                        return Boolean.FALSE;
                    } else {
                        if (expectedValue.equals(oldObject)) {
                            txn.attachPutOp(name, key, toData(newValue), false);
                            return Boolean.TRUE;
                        } else {
                            return Boolean.FALSE;
                        }
                    }
                } else {
                    if (expectedValue.equals(toObject(txn.get(name, key)))) {
                        txn.attachPutOp(name, key, toData(newValue), false);
                        return Boolean.TRUE;
                    } else {
                        return Boolean.FALSE;
                    }
                }
            } else {
                Data dataExpected = toData(expectedValue);
                Data dataNew = toData(newValue);
                setLocal(operation, name, key, new MultiData(dataExpected, dataNew), -1, -1);
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
            return txnalPut(operation, name, key, value, timeout, ttl, -1);
        }

        Object txnalPut(ClusterOperation operation, String name, Object key, Object value, long timeout, long ttl, long txnId) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getTransaction();
            SystemLogService css = node.getSystemLogService();
            if (css.shouldLog(INFO)) {
                css.logObject(MPut.this, INFO, operation);
            }
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock
                            .lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) {
                        throwTxTimeoutException(key);
                    }
                    Object oldObject = null;
                    Data oldValue = mlock.oldValue;
                    if (oldValue != null) {
                        oldObject = threadContext.isClient() ? oldValue : threadContext.toObject(oldValue);
                    }
                    if (operation == CONCURRENT_MAP_PUT_IF_ABSENT && oldObject != null) {
                        txn.attachPutOp(name, key, oldValue, 0, ttl, false);
                    } else {
                        txn.attachPutOp(name, key, toData(value), 0, ttl, (oldObject == null));
                    }
                    if (operation == CONCURRENT_MAP_TRY_PUT) {
                        return Boolean.TRUE;
                    }
                    return oldObject;
                } else {
                    if (operation == CONCURRENT_MAP_PUT_IF_ABSENT) {
                        Data existingValue = txn.get(name, key);
                        if (existingValue != null) {
                            return threadContext.isClient() ? existingValue : threadContext.toObject(existingValue);
                        }
                    }
                    Data resultData = txn.attachPutOp(name, key, toData(value), false);
                    if (operation == CONCURRENT_MAP_TRY_PUT) {
                        return Boolean.TRUE;
                    }
                    return threadContext.isClient() ? resultData : threadContext.toObject(resultData);
                }
            } else {
                setLocal(operation, name, key, value, timeout, ttl);
                request.txnId = txnId;
                setIndexValues(request, value);
                if (operation == CONCURRENT_MAP_TRY_PUT
                        || operation == CONCURRENT_MAP_SET
                        || operation == CONCURRENT_MAP_PUT_AND_UNLOCK
                        || operation == CONCURRENT_MAP_PUT_FROM_LOAD
                        || operation == CONCURRENT_MAP_PUT_TRANSIENT) {
                    request.setBooleanRequest();
                    Data valueData = request.value;
                    doOp();
                    Boolean successful = getResultAsBoolean();
                    if (successful) {
                        request.value = valueData;
                        if (operation == CONCURRENT_MAP_PUT_AND_UNLOCK) {
                            backup(CONCURRENT_MAP_BACKUP_PUT_AND_UNLOCK);
                        } else {
                            backup(CONCURRENT_MAP_BACKUP_PUT);
                        }
                    }
                    return successful;
                } else {
                    request.setObjectRequest();
                    if (css.shouldLog(TRACE)) {
                        css.logObject(MPut.this, TRACE, "Calling doOp");
                    }
                    doOp();
                    if (css.shouldLog(TRACE)) {
                        css.logObject(MPut.this, TRACE, "Done doOp");
                    }
                    Object returnObject = getResultAsObject();
                    if (css.shouldLog(INFO)) {
                        css.logObject(MPut.this, INFO, returnObject);
                    }
                    if (operation == CONCURRENT_MAP_REPLACE_IF_NOT_NULL && returnObject == null) {
                        return null;
                    }
                    if (returnObject instanceof AddressAwareException) {
                        rethrowException(operation, (AddressAwareException) returnObject);
                    }
                    request.longValue = Long.MIN_VALUE;
                    backup(CONCURRENT_MAP_BACKUP_PUT);
                    if (css.shouldLog(TRACE)) {
                        css.logObject(MPut.this, TRACE, "Backups completed returning result");
                    }
                    return returnObject;
                }
            }
        }

        @Override
        protected final boolean canTimeout() {
            switch (request.operation) {
                case CONCURRENT_MAP_PUT_AND_UNLOCK:
                case CONCURRENT_MAP_BACKUP_PUT_AND_UNLOCK:
                    return false;
                default:
                    return true;
            }
        }

        @Override
        protected boolean shouldRedoWhenOwnerDies() {
            return true;
        }
    }

    class MRemoveMulti extends MBackupAndMigrationAwareOp {

        public Collection remove(String name, Object key) {
            final ThreadContext tc = ThreadContext.get();
            TransactionImpl txn = tc.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                Collection committedValues = null;
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, DEFAULT_TXN_TIMEOUT);
                    if (!locked) throwTxTimeoutException(key);
                    committedValues = (Collection) toObject(mlock.oldValue);
                } else {
                    Object value = objectCall(CONCURRENT_MAP_GET, name, key, null, 0, -1);
                    if (value instanceof AddressAwareException) {
                        rethrowException(request.operation, (AddressAwareException) value);
                    }
                    committedValues = (Collection) value;
                }
                List allValues = new ArrayList();
                int removedValueCount = 1;
                if (committedValues != null) {
                    allValues.addAll(committedValues);
                    removedValueCount = committedValues.size();
                }
                txn.getMulti(name, key, allValues);
                txn.attachRemoveOp(name, key, null, false, removedValueCount);
                return allValues;
            } else {
                Collection result = (Collection) objectCall(CONCURRENT_MAP_REMOVE, name, key, null, -1, -1);
                if (result != null) {
                    backup(CONCURRENT_MAP_BACKUP_REMOVE);
                }
                return result;
            }
        }

        boolean remove(String name, Object key, Object value) {
            ThreadContext threadContext = ThreadContext.get();
            TransactionImpl txn = threadContext.getCallContext().getTransaction();
            if (txn != null && txn.getStatus() == Transaction.TXN_STATUS_ACTIVE) {
                if (!txn.has(name, key)) {
                    MLock mlock = new MLock();
                    boolean locked = mlock.lockAndGetValue(name, key, value, DEFAULT_TXN_TIMEOUT);
                    if (!locked) throwTxTimeoutException(key);
                    Data oldValue = mlock.oldValue;
                    boolean existingRecord = (oldValue != null);
                    txn.attachRemoveOp(name, key, toData(value), !existingRecord);
                    return existingRecord;
                } else {
                    MContainsKey mContainsKey = new MContainsKey();
                    boolean containsEntry = mContainsKey.containsEntry(name, key, value);
                    txn.attachRemoveOp(name, key, toData(value), !containsEntry);
                    return containsEntry;
                }
            } else {
                boolean result = booleanCall(CONCURRENT_MAP_REMOVE_MULTI, name, key, value, -1, -1);
                if (result) {
                    backup(CONCURRENT_MAP_BACKUP_REMOVE_MULTI);
                }
                return result;
            }
        }

        @Override
        protected boolean shouldRedoWhenOwnerDies() {
            return true;
        }
    }

    abstract class MBackupAndMigrationAwareOp extends MBackupAwareOp {
        @Override
        public boolean isMigrationAware() {
            return true;
        }
    }

    abstract class MDefaultBackupAndMigrationAwareOp extends MBackupAndMigrationAwareOp {
        @Override
        void prepareForBackup() {
            backupCount = Math.min(MapConfig.DEFAULT_BACKUP_COUNT, dataMemberCount.get() - 1);
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

    protected Address getBackupMember(final int partitionId, final int replicaIndex) {
        return partitionManager.getPartition(partitionId).getReplicaAddress(replicaIndex);
    }

    class MBackup extends MTargetAwareOp {
        protected int replicaIndex = 0;

        public void sendBackup(ClusterOperation operation, int replicaIndex, Request reqBackup) {
            reset();
            this.replicaIndex = replicaIndex;
            SystemLogService css = node.getSystemLogService();
            if (css.shouldLog(TRACE)) {
                css.trace(this, "SendingBackup callId.", callId);
            }
            request.setFromRequest(reqBackup);
            request.operation = operation;
            request.caller = thisAddress;
            request.longValue = replicaIndex;
            request.setBooleanRequest();
            doOp();
        }

        public void reset() {
            super.reset();
            replicaIndex = 0;
        }

        @Override
        public void process() {
            target = getBackupMember(request.blockId, replicaIndex);
            if (target == null) {
                if (backupRedoEnabled && isValidBackup()) {
                    setRedoResult(REDO_TARGET_UNKNOWN);
                } else {
                    setResult(Boolean.FALSE);
                }
            } else {
                if (target.equals(thisAddress)) {
                    doLocalOp();
                } else {
                    invoke();
                }
            }
        }

        // executed by ServiceThread
        boolean isValidBackup() {
            int maxBackupCount = dataMemberCount.get() - 1;
            if (maxBackupCount > 0) {
                CMap map = getOrCreateMap(request.name);
                maxBackupCount = Math.min(map.getBackupCount(), maxBackupCount);
            }
            maxBackupCount = maxBackupCount > 0 ? maxBackupCount : 0;
            return replicaIndex <= maxBackupCount;
        }

        boolean isMigrationAware() {
            return backupRedoEnabled;
        }

        boolean isPartitionMigrating() {
            return isMigrating(request, replicaIndex);
        }

        @Override
        protected final void handleInterruption() {
            logger.log(Level.WARNING, Thread.currentThread().getName() + " is interrupted! " +
                    "Hazelcast intentionally suppresses interruption during backup operations. " +
                    "Operation: " + request.operation);
        }
    }

    class AsyncBackupProcessable implements Processable {
        final Request request;
        final int replicaIndex;

        AsyncBackupProcessable(final Request request, final int replicaIndex) {
            this.request = request;
            this.replicaIndex = replicaIndex;
        }

        public void process() {
            final Address target = getBackupMember(request.blockId, replicaIndex);
            if (target != null) {
                if (thisAddress.equals(target)) {
                    processBackupRequest(request);
                } else {
                    final Packet packet = obtainPacket();
                    packet.setFromRequest(request);
                    // this is not a call! we do not expect any response!
                    // @see BackupOperationHandler
                    packet.callId = -1L;
                    sendOrReleasePacket(packet, target);
                }
            }
        }
    }

    abstract class MBackupAwareOp extends MTargetAwareOp {
        protected volatile int backupCount = 0;
        protected volatile int asyncBackupCount = 0;

        protected void backup(ClusterOperation operation) {
            final int localBackupCount = backupCount;
            final int localAsyncBackupCount = asyncBackupCount;
            final int totalBackupCount = localBackupCount + localAsyncBackupCount;
            if (localBackupCount <= 0 && localAsyncBackupCount <= 0) {
                return;
            }
            if (totalBackupCount > maxBackupCount) {
                String msg = "Max backup is " + maxBackupCount + " but total backupCount is " + totalBackupCount;
                logger.log(Level.SEVERE, msg);
                throw new HazelcastException(msg);
            }
            if (request.key == null || request.key.size() == 0) {
                throw new HazelcastException("Key is null! " + request.key);
            }
            final MBackup[] backupOps = new MBackup[localBackupCount];
            for (int i = 0; i < totalBackupCount; i++) {
                final int replicaIndex = i + 1;
                if (i < localBackupCount) {
                    MBackup backupOp = new MBackup();
                    backupOps[i] = backupOp;
                    backupOp.sendBackup(operation, replicaIndex, request);
                } else {
                    final Request reqBackup = Request.copyFromRequest(request);
                    reqBackup.operation = operation;
                    enqueueAndReturn(new AsyncBackupProcessable(reqBackup, replicaIndex));
                }
            }
            for (int i = 0; i < localBackupCount; i++) {
                MBackup backupOp = backupOps[i];
                try {
                    if (!backupOp.getResultAsBoolean()) {
                        if (logger.isLoggable(Level.FINEST)) {
                            logger.log(Level.FINEST, "Backup failed -> " + request);
                        }
                    }
                } catch (HazelcastException e) {
                    final Level level = backupRedoEnabled ? Level.WARNING : Level.FINEST;
                    logger.log(level, "Backup operation [" + operation + "] has failed! "
                            + e.getClass().getName() + ": " + e.getMessage());
                    logger.log(Level.FINEST, e.getMessage(), e);
                }
            }
            if (totalBackupCount > 0 && shouldRedoWhenOwnerDies()
                    && target != null && node.getClusterImpl().getMember(target) == null) {
                // Operation seems successful but since owner target is dead, we may loose data!
                // We should retry actual operation for the new target
                logger.log(Level.WARNING, "Target[" + target + "] is dead! " +
                        "Hazelcast will retry " + request.operation);
                // TODO: what if another call changes actual value? Do we need version check?
                doOp(); // means redo...
                getRedoAwareResult();   // wait for operation to complete...
            }
        }

        protected boolean shouldRedoWhenOwnerDies() {
            return false;
        }

        // executed by ServiceThread
        void prepareForBackup() {
            int localBackupCount = 0;
            int localAsyncBackupCount = 0;
            final int maxBackup = dataMemberCount.get() - 1;
            if (maxBackup > 0) {
                CMap map = getOrCreateMap(request.name);
                localBackupCount = Math.min(map.getBackupCount(), maxBackup);
                localAsyncBackupCount = Math.min(map.getAsyncBackupCount(), (maxBackup - localBackupCount));
            }
            backupCount = localBackupCount > 0 ? localBackupCount : 0;
            asyncBackupCount = localAsyncBackupCount > 0 ? localAsyncBackupCount : 0;
        }

        @Override
        public void process() {
            prepareForBackup();
            request.blockId = getPartitionId(request);
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
            request.blockId = -1;
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
        fireMapEvent(mapListeners, record.getName(), eventType, record.getKeyData(),
                oldValue, record.getValueData(), record.getListeners(), callerAddress);
    }

    public class MClearQuick extends MultiCall<Boolean> {
        final String name;
        boolean result;


        public MClearQuick(String name) {
            this.name = name;
        }

        @Override
        SubCall createNewTargetAwareOp(Address target) {
            return new MTargetClearQuickMap(target);
        }

        @Override
        boolean onResponse(Object response) {
            return true;
        }

        @Override
        Object returnResult() {
            return result;
        }

        @Override
        void onComplete() {
            this.result = true;
        }

        @Override
        protected boolean excludeLiteMember() {
            return true;
        }

        class MTargetClearQuickMap extends SubCall {
            public MTargetClearQuickMap(Address target) {
                super(target);
                setLocal(CONCURRENT_MAP_CLEAR_QUICK, name, null, null, 0, -1);
                request.setBooleanRequest();
            }
        }
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

        protected boolean excludeLiteMember() {
            return true;
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

        protected boolean excludeLiteMember() {
            return true;
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

            @Override
            protected final boolean canTimeout() {
                return false;
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
                long now = currentTimeMillis();
                for (Record record : cMap.mapRecords.values()) {
                    if (record.isActive() && record.isValid(now) && record.hasValueData()) {
                        if (cMap.isReadBackupData()) {
                            return false;
                        } else {
                            Partition partition = partitionServiceImpl.getPartition(record.getBlockId());
                            if (partition != null && partition.getOwner() != null && partition.getOwner().localMember()) {
                                return false;
                            }
                        }
                    }
                }
            }
            return size(name) == 0;
        }
    }

    public LocalMapStatsImpl getLocalMapStats(String name) {
        final CMap cmap = getMap(name);
        if (cmap == null) {
            return new LocalMapStatsImpl();
        }
        return cmap.getLocalMapStats();
    }

    public Address getKeyOwner(Request req) {
        int partitionId = getPartitionId(req);
        return getPartitionOwner(partitionId);
    }

    public Address getPartitionOwner(int partitionId) {
        return partitionManager.getOwner(partitionId);
    }

    public Address getKeyOwner(Data key) {
        int partitionId = getPartitionId(key);
        return getPartitionOwner(partitionId);
    }

    public boolean isMigrating(Request req) {
        return isMigrating(req, 0);
    }

    @Override
    public boolean isMigrating(Request req, int replica) {
        final Data key = req.key;
        return key != null && partitionManager.isPartitionMigrating(getPartitionId(req), replica);
    }

    public int getPartitionId(Request req) {
        req.blockId = getPartitionId(req.key);
        return req.blockId;
    }

    public final int getPartitionId(Data key) {
        int hash = key.getPartitionHash();
        return (hash == Integer.MIN_VALUE) ? 0 : Math.abs(hash) % partitionCount;
    }

    public long newRecordId() {
        checkServiceThread();
        return newRecordId++;
    }

    void evictAsync(final String name, final Data key) {
        evictionExecutor.execute(new FallThroughRunnable() {
            public void doRun() {
                MEvict mEvict = new MEvict();
                mEvict.evict(name, key);
            }
        });
    }

    public CMap getMap(String name) {
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
    void registerListener(boolean add, String name, Data key, Address address, boolean includeValue) {
        CMap cmap = getOrCreateMap(name);
        if (add) {
            cmap.addListener(key, address, includeValue);
        } else {
            cmap.removeListener(key, address);
        }
    }

    class LockMapOperationHandler extends MigrationAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.lockMap(request);
        }
    }

    class BackupOperationHandler extends TargetAwareOperationHandler {

        boolean isCallerKnownMember(Request request) {
            return !backupRedoEnabled || super.isCallerKnownMember(request);
        }

        boolean isRightRemoteTarget(final Request request) {
            if (!backupRedoEnabled) {
                return true;
            }
            final int partitionId = getPartitionId(request);
            final PartitionInfo partition = partitionManager.getPartition(partitionId);
            return thisAddress.equals(partition.getReplicaAddress(getReplicaIndex(request)));
        }

        boolean isPartitionMigrating(final Request request) {
            return backupRedoEnabled && isMigrating(request, getReplicaIndex(request));
        }

        private int getReplicaIndex(final Request request) {
            return (int) request.longValue;
        }

        public void handle(Request request) {
            doOperation(request);
            // If request is not a Call, no need to return a response
            // @see AsyncBackupProcessable
            if (request.callId != -1) {
                returnResponse(request);
            }
        }

        void doOperation(Request request) {
            Boolean value = processBackupRequest(request);
            request.clearForResponse();
            request.response = value;
        }
    }

    /**
     * Should be called by only ServiceThread
     */
    private boolean processBackupRequest(Request request) {
        CMap cmap = getOrCreateMap(request.name);
        return cmap.backup(request);
    }

    class AsyncMergePacketProcessor implements PacketProcessor {
        public void process(final Packet packet) {
            packet.operation = CONCURRENT_MAP_WAN_MERGE;
            final Data key = packet.getKeyData();
            Address address = getKeyOwner(key);
            if (thisAddress.equals(address)) {
                WanMergePacketProcessor p = (WanMergePacketProcessor) getPacketProcessor(CONCURRENT_MAP_WAN_MERGE);
                p.process(packet);
            } else {
                sendOrReleasePacket(packet, address);
            }
        }
    }

    class WanMergePacketProcessor implements PacketProcessor {
        final ParallelExecutor parallelExecutor = node.executorManager.newParallelExecutor(20);

        public void process(final Packet packet) {
            final DataRecordEntry dataRecordEntry = (DataRecordEntry) toObject(packet.getValueData());
            node.concurrentMapManager.getOrCreateMap(packet.name);
            parallelExecutor.execute(new Runnable() {
                public void run() {
                    mergeWanRecord(dataRecordEntry);
                }
            });
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
            return thisAddress.equals(getKeyOwner(request));
        }
    }

    class RemoveItemOperationHandler extends RemoveOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.removeItem(request);
        }

        // removeItem returns boolean, no need to throw timeout exception!
        protected void onNoTimeToSchedule(Request request) {
            request.response = Boolean.FALSE;
            returnResponse(request);
        }
    }

    class ClearQuickOperationHandler extends MigrationAwareOperationHandler {

        @Override
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.clearQuick();
            request.response = true;
            returnResponse(request);
        }

        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                doOperation(request);
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }
    }

    class RemoveOperationHandler extends SchedulableOperationHandler {

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            cmap.remove(request);
        }

        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                if (shouldSchedule(request)) {
                    if (request.hasEnoughTimeToSchedule()) {
                        if (systemLogService.shouldLog(INFO)) {
                            systemLogService.info(request,
                                    MapSystemLogFactory.newScheduleRequest(request, cmap.getRecord(request)));
                        }
                        schedule(request);
                    } else {
                        if (systemLogService.shouldLog(INFO)) {
                            systemLogService.info(request, "NoTimeToSchedule", request.name, request.operation);
                        }
                        onNoTimeToSchedule(request);
                    }
                    return;
                }
                Record record = cmap.getRecord(request);
                if ((record == null || record.isLoadable()) && cmap.loader != null) {
                    storeExecutor.execute(new RemoveLoader(cmap, request), request.key.hashCode());
                } else {
                    storeProceed(cmap, request);
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        class RemoveLoader extends AbstractMapStoreOperation {
            Data valueData = null;

            RemoveLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object key = toObject(request.key);
                Object value = cmap.loader.load(key);
                valueData = toData(value);
            }

            public void process() {
                Record record = cmap.getRecord(request);
                if (valueData != null) {
                    if (record == null) {
                        record = cmap.createAndAddNewRecord(request.key, valueData);
                    } else {
                        record.setValueData(valueData);
                    }
                    record.setActive();
                }

                if (record != null) {
                    if (record.isActive() && !record.isValid()) {
                        // record is not valid, it is waiting for eviction.
                        // we should cancel eviction by making record valid
                        // and proceed to standard remove operation.
                        record.setExpirationTime(Long.MAX_VALUE);
                        record.setMaxIdle(Long.MAX_VALUE);
                    }
                    storeProceed(cmap, request);
                } else {
                    returnResponse(request);
                }
            }
        }

        void storeProceed(CMap cmap, Request request) {
            if (cmap.store != null && cmap.writeDelayMillis == 0) {
                storeExecutor.execute(new RemoveStorer(cmap, request), request.key.hashCode());
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }

        class RemoveStorer extends AbstractMapStoreOperation {

            RemoveStorer(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object key = toObject(request.key);
                cmap.store.delete(key);
                afterMapStore();
            }

            public void process() {
                if (success) doOperation(request);
                returnResponse(request);
            }
        }
    }

    class RemoveMultiOperationHandler extends SchedulableOperationHandler {

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (record == null || record.getMultiValues() == null) {
                request.response = Boolean.FALSE;
                returnResponse(request);
            } else {
                storeExecutor.execute(new RemoveMultiSetMapTask(request, record, cmap), request.key.hashCode());
            }
        }

        class RemoveMultiSetMapTask implements Runnable, Processable {
            final CMap cmap;
            final Request request;
            final Record record;

            RemoveMultiSetMapTask(Request request, Record record, CMap cmap) {
                this.request = request;
                this.record = record;
                this.cmap = cmap;
            }

            public void run() {
                final Collection<ValueHolder> multiValues = record.getMultiValues();
                if (multiValues == null) {
                    request.response = Boolean.FALSE;
                    returnResponse(request);
                } else {
                    request.response = multiValues.remove(new ValueHolder(request.value));
                    enqueueAndReturn(RemoveMultiSetMapTask.this);
                }
            }

            public void process() {
                if (request.response == Boolean.TRUE) {
                    cmap.onRemoveMulti(request, record);
                }
                returnResponse(request);
            }
        }
    }

    class PutMultiOperationHandler extends SchedulableOperationHandler {

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (!cmap.multiMapSet) {
                cmap.putMulti(request);
                request.response = Boolean.TRUE;
                returnResponse(request);
            } else {
                Record record = cmap.getRecord(request);
                if (record == null || record.getMultiValues() == null || !record.isValid()) {
                    cmap.putMulti(request);
                    request.response = Boolean.TRUE;
                    returnResponse(request);
                } else {
                    storeExecutor.execute(new PutMultiSetMapTask(request, record, cmap), request.key.hashCode());
                }
            }
        }

        class PutMultiSetMapTask implements Runnable, Processable {
            final CMap cmap;
            final Request request;
            final Record record;

            PutMultiSetMapTask(Request request, Record record, CMap cmap) {
                this.request = request;
                this.record = record;
                this.cmap = cmap;
            }

            public void run() {
                request.response = Boolean.TRUE;
                final Collection<ValueHolder> multiValues = record.getMultiValues();
                if (multiValues != null) {
                    request.response = !multiValues.contains(new ValueHolder(request.value));
                }
                enqueueAndReturn(PutMultiSetMapTask.this);
            }

            public void process() {
                if (request.response == Boolean.TRUE) {
                    cmap.putMulti(request);
                }
                returnResponse(request);
            }
        }
    }

    class ReplaceOperationHandler extends SchedulableOperationHandler {

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (record == null) {
                request.response = Boolean.FALSE;
                returnResponse(request);
            } else {
                storeExecutor.execute(new ReplaceTask(request, record, cmap), request.key.hashCode());
            }
        }

        class ReplaceTask extends AbstractMapStoreOperation {
            final Record record;

            ReplaceTask(Request request, Record record, CMap cmap) {
                super(cmap, request);
                this.record = record;
            }

            public void doMapStoreOperation() {
                MultiData multiData = (MultiData) toObject(request.value);
                Object expectedValue = toObject(multiData.getData(0));
                request.value = multiData.getData(1); // new value
                request.response = expectedValue.equals(record.getValue());

                if (request.response == Boolean.TRUE) {
                    // to prevent possible race condition!
                    // See testMapReplaceIfSame# tests in ClusterTest
                    record.setValueData(request.value);
                }
                if (cmap.store != null && cmap.writeDelayMillis == 0) {
                    cmap.store.store(toObject(request.key), toObject(request.value));
                    afterMapStore();
                }
            }

            public void process() {
                if (request.response == Boolean.TRUE) {
                    cmap.put(request);
                    request.response = Boolean.TRUE;
                }
                request.value = null;
                returnResponse(request);
            }
        }
    }

    class RemoveIfSameOperationHandler extends SchedulableOperationHandler {

        public void handle(Request request) {
            if (shouldSchedule(request)) {
                if (request.hasEnoughTimeToSchedule()) {
                    schedule(request);
                } else {
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (record == null) {
                request.response = Boolean.FALSE;
                returnResponse(request);
            } else {
                storeExecutor.execute(new RemoveIfSameTask(request, record, cmap), request.key.hashCode());
            }
        }

        class RemoveIfSameTask extends AbstractMapStoreOperation {
            final Record record;

            RemoveIfSameTask(Request request, Record record, CMap cmap) {
                super(cmap, request);
                this.record = record;
            }

            public void doMapStoreOperation() {
                Object expectedValue = toObject(request.value);
                request.response = expectedValue.equals(record.getValue());
                if (cmap.store != null && cmap.writeDelayMillis == 0) {
                    cmap.store.delete(toObject(request.key));
                    afterMapStore();
                }
            }

            public void process() {
                request.value = null;
                if (request.response == Boolean.TRUE && record.isActive()) {
                    // return true only if record is actually removed
                    // (see testMapRemoveIfSame test)
                    cmap.remove(request);
                    request.response = Boolean.TRUE;
                } else {
                    request.response = Boolean.FALSE;
                }
                returnResponse(request);
            }
        }
    }

    class PutTransientOperationHandler extends SchedulableOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (!cmap.isNotLocked(request)) {
                setRedoResponse(request, REDO_MAP_LOCKED);
            } else if (cmap.overCapacity()) {
                setRedoResponse(request, REDO_MAP_OVER_CAPACITY);
            } else {
                Record record = ensureRecord(request);
                boolean dirty = (record != null) && record.isDirty();
                cmap.put(request);
                if (record != null) {
                    record.setDirty(dirty);
                    if (!dirty) {
                        record.setLastStoredTime(Clock.currentTimeMillis());
                    }
                }
                request.value = null;
                request.response = Boolean.TRUE;
            }
        }
    }

    class PutFromLoadOperationHandler extends SchedulableOperationHandler {
        protected void onNoTimeToSchedule(Request request) {
            request.response = Boolean.FALSE;
            returnResponse(request);
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.overCapacity()) {
                request.value = null;
                request.response = Boolean.FALSE;
            } else {
                Record record = ensureRecord(request);
                cmap.put(request);
                if (record != null) {
                    record.setDirty(false);
                    record.setLastStoredTime(Clock.currentTimeMillis());
                }
                request.value = null;
                request.response = Boolean.TRUE;
            }
        }
    }

    class PutOperationHandler extends SchedulableOperationHandler {
//        @Override
//        protected void onNoTimeToSchedule(Request request) {
//            request.response = null;
//            if (request.operation == CONCURRENT_MAP_TRY_PUT
//                    || request.operation == CONCURRENT_MAP_PUT_AND_UNLOCK
//                    || request.operation == CONCURRENT_MAP_SET) {
//                request.response = Boolean.FALSE;
//            }
//            returnResponse(request);
//        }

        @Override
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            SystemLogService css = node.getSystemLogService();
            if (css.shouldLog(TRACE)) {
                css.logObject(request, TRACE, "Calling cmap.put");
            }
            cmap.put(request);
            if (request.operation == CONCURRENT_MAP_TRY_PUT
                    || request.operation == CONCURRENT_MAP_PUT_AND_UNLOCK) {
                request.response = Boolean.TRUE;
            }
            if (css.shouldLog(INFO)) {
                css.info(request, "req.response", request.response);
            }
        }

        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            SystemLogService css = systemLogService;
            if (css.shouldLog(TRACE)) {
                css.logObject(request, TRACE, cmap);
            }
            boolean checkCapacity = request.operation != CONCURRENT_MAP_REPLACE_IF_NOT_NULL;
            boolean overCapacity = checkCapacity && cmap.overCapacity();
            boolean cmapNotLocked = cmap.isNotLocked(request);
            if (css.shouldLog(TRACE)) {
                css.trace(request, "OverCapacity/CmapNotLocked", overCapacity, cmapNotLocked);
            }
            if (cmapNotLocked) {
                if (!overCapacity) {
                    if (shouldSchedule(request)) {
                        if (request.hasEnoughTimeToSchedule()) {
                            if (css.shouldLog(INFO)) {
                                css.info(request, MapSystemLogFactory.newScheduleRequest(request, cmap.getRecord(request)));
                            }
                            schedule(request);
                        } else {
                            if (css.shouldLog(INFO)) {
                                css.info(request, "NoTimeToSchedule", request.name, request.operation);
                            }
                            onNoTimeToSchedule(request);
                        }
                        return;
                    }
                    Record record = cmap.getRecord(request);
                    if (css.shouldLog(TRACE)) {
                        css.trace(request, "Record is", record);
                    }
                    if ((record == null || record.isLoadable()) && cmap.loader != null
                            && request.operation != ClusterOperation.CONCURRENT_MAP_SET) {
                        if (css.shouldLog(TRACE)) {
                            css.trace(request, "Will Load");
                        }
                        storeExecutor.execute(new PutLoader(cmap, request), request.key.hashCode());
                    } else {
                        storeProceed(cmap, request);
                    }
                } else if (request.operation == CONCURRENT_MAP_TRY_PUT) { // over capacity and tryPut
                    request.response = Boolean.FALSE;
                    returnResponse(request);
                } else {
                    returnRedoResponse(request, REDO_MAP_OVER_CAPACITY); // overcapacity and put
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);  // cmap locked
            }
        }

        class PutLoader extends AbstractMapStoreOperation {
            Data valueData = null;

            PutLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object key = toObject(request.key);
                Object value = cmap.loader.load(key);
                valueData = toData(value);
            }

            public void process() {
                if (valueData != null) {
                    Record record = cmap.getRecord(request);
                    if (record == null) {
                        record = cmap.createAndAddNewRecord(request.key, valueData);
                    } else {
                        record.setValueData(valueData);
                    }
                }
                storeProceed(cmap, request);
            }
        }

        void storeProceed(CMap cmap, Request request) {
            if (cmap.store != null && cmap.writeDelayMillis == 0
                    && cmap.isApplicable(request.operation, request, Clock.currentTimeMillis())) {
                storeExecutor.execute(new PutStorer(cmap, request), request.key.hashCode());
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }

        class PutStorer extends AbstractMapStoreOperation {

            PutStorer(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value;
                if (request.operation == CONCURRENT_MAP_REPLACE_IF_SAME) {
                    MultiData multiData = (MultiData) toObject(request.value);
                    value = toObject(multiData.getData(1));
                } else {
                    value = toObject(request.value);
                }
                Object key = toObject(request.key);
                cmap.store.store(key, value);
                afterMapStore();
            }

            public void process() {
                if (success) doOperation(request);
                returnResponse(request);
            }
        }
    }

    abstract class AtomicNumberOperationHandler extends MTargetAwareOperationHandler {
        abstract long getNewValue(long oldValue, long value);

        abstract long getResponseValue(long oldValue, long value);

        @Override
        void doOperation(Request request) {
            final Record record = ensureRecord(request, AtomicNumberProxy.DATA_LONG_ZERO);
            final Data oldValueData = record.getValueData();
            final Data expectedValue = request.value;
            final long value = request.longValue;
            request.clearForResponse();
            if (expectedValue == null || expectedValue.equals(oldValueData)) {
                final long oldValue = (Long) toObject(oldValueData);
                final long newValue = getNewValue(oldValue, value);
                request.longValue = getResponseValue(oldValue, value);
                if (oldValue != newValue) {
                    record.setValueData(toData(newValue));
                    record.incrementVersion();
                    request.version = record.getVersion();
                    request.response = record.getValueData();
                }
            } else {
                request.longValue = 0L;
            }
        }
    }

    class AtomicNumberAddAndGetOperationHandler extends AtomicNumberOperationHandler {
        long getNewValue(long oldValue, long value) {
            return oldValue + value;
        }

        long getResponseValue(long oldValue, long value) {
            return oldValue + value;
        }
    }

    class AtomicNumberGetAndAddOperationHandler extends AtomicNumberOperationHandler {
        long getNewValue(long oldValue, long value) {
            return oldValue + value;
        }

        long getResponseValue(long oldValue, long value) {
            return oldValue;
        }
    }

    class AtomicNumberGetAndSetOperationHandler extends AtomicNumberOperationHandler {
        long getNewValue(long oldValue, long value) {
            return value;
        }

        long getResponseValue(long oldValue, long value) {
            return oldValue;
        }
    }

    class AtomicNumberCompareAndSetOperationHandler extends AtomicNumberOperationHandler {
        long getNewValue(long oldValue, long value) {
            return value;
        }

        long getResponseValue(long oldValue, long value) {
            return 1L;
        }
    }

    abstract class CountDownLatchOperationHandler extends SchedulableOperationHandler {
        abstract void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl);

        @Override
        public void handle(Request request) {
            request.record = ensureRecord(request, DistributedCountDownLatch.newInstanceData);
            doOperation(request);
        }

        @Override
        void doOperation(Request request) {
            doCountDownLatchOperation(request, (DistributedCountDownLatch) request.record.getValue());
        }

        @Override
        protected void onNoTimeToSchedule(Request request) {
            doResponse(request, null, CountDownLatchProxy.AWAIT_FAILED, false);
        }

        protected void doResponse(Request request, DistributedCountDownLatch cdl, long retValue, boolean changed) {
            final Record record = request.record;
            request.clearForResponse();
            if (changed) {
                record.setValueData(toData(cdl));
                record.incrementVersion();
                request.version = record.getVersion();
                request.response = record.getValueData();
            }
            request.longValue = retValue;
            if (changed && request.operation == COUNT_DOWN_LATCH_COUNT_DOWN && cdl.getCount() == 0) {
                request.longValue = releaseThreads(record);
            }
            returnResponse(request);
        }

        private int releaseThreads(Record record) {
            int threadsReleased = 0;
            final List<ScheduledAction> scheduledActions = record.getScheduledActions();
            if (scheduledActions != null) {
                for (ScheduledAction sa : scheduledActions) {
                    node.clusterManager.deregisterScheduledAction(sa);
                    if (!sa.expired()) {
                        sa.consume();
                        ++threadsReleased;
                    } else {
                        sa.onExpire();
                    }
                }
                scheduledActions.clear();
            }
            return threadsReleased;
        }
    }

    class CountDownLatchAwaitOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            if (cdl.ownerLeft()) {
                request.clearForResponse();
                doResponse(request, null, CountDownLatchProxy.OWNER_LEFT, false);
            } else if (cdl.getCount() == 0) {
                request.clearForResponse();
                doResponse(request, null, CountDownLatchProxy.AWAIT_DONE, false);
            } else {
                request.lockThreadId = ThreadContext.get().getThreadId();
                schedule(request);
            }
        }
    }

    class CountDownLatchCountDownOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            doResponse(request, cdl, 0, cdl.countDown());
        }
    }

    class CountDownLatchDestroyOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            final List<ScheduledAction> scheduledActions = request.record.getScheduledActions();
            if (scheduledActions != null) {
                for (ScheduledAction sa : scheduledActions) {
                    node.clusterManager.deregisterScheduledAction(sa);
                    doResponse(sa.getRequest(), null, CountDownLatchProxy.INSTANCE_DESTROYED, false);
                }
            }
            request.clearForResponse();
            returnResponse(request);
        }
    }

    class CountDownLatchGetCountOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            doResponse(request, cdl, cdl.getCount(), false);
        }
    }

    class CountDownLatchGetOwnerOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            request.clearForResponse();
            request.response = cdl.getOwnerAddress();
            returnResponse(request);
        }
    }

    class CountDownLatchSetCountOperationHandler extends CountDownLatchOperationHandler {
        void doCountDownLatchOperation(Request request, DistributedCountDownLatch cdl) {
            boolean countSet = cdl.setCount((int) request.longValue, request.caller, request.lockAddress);
            doResponse(request, cdl, (countSet ? 1 : 0), countSet);
        }
    }

    abstract class SemaphoreOperationHandler extends SchedulableOperationHandler {
        abstract void doSemaphoreOperation(Request request, DistributedSemaphore semaphore);

        @Override
        public void handle(final Request request) {
            request.record = ensureRecord(request, null);
            if (request.record.getValue() == null) {
                final String name = (String) toObject(request.key);
                final SemaphoreConfig sc = node.getConfig().getSemaphoreConfig(name);
                final int configInitialPermits = sc.getInitialPermits();
                if (sc.isFactoryEnabled()) {
                    node.executorManager.executeNow(new Runnable() {
                        public void run() {
                            try {
                                initSemaphore(sc, request, name);
                            } catch (Exception e) {
                                logger.log(Level.SEVERE, e.getMessage(), e);
                            } finally {
                                enqueueAndReturn(new Processable() {
                                    public void process() {
                                        SemaphoreOperationHandler.this.handle(request);
                                    }
                                });
                            }
                        }
                    });
                    return;
                } else {
                    request.record.setValue(new DistributedSemaphore(configInitialPermits));
                }
            }
            doOperation(request);
        }

        synchronized void initSemaphore(SemaphoreConfig sc, Request request, String name) throws Exception {
            if (request.record.getValue() == null) {
                final int configInitialPermits = sc.getInitialPermits();
                SemaphoreFactory factory = sc.getFactoryImplementation();
                if (factory == null) {
                    String factoryClassName = sc.getFactoryClassName();
                    if (factoryClassName != null && factoryClassName.length() != 0) {
                        ClassLoader cl = node.getConfig().getClassLoader();
                        Class factoryClass = Serializer.loadClass(cl, factoryClassName);
                        factory = (SemaphoreFactory) factoryClass.newInstance();
                    }
                }
                if (factory != null) {
                    int initialPermits = factory.getInitialPermits(name, configInitialPermits);
                    request.record.setValue(new DistributedSemaphore(initialPermits));
                }
            }
        }

        @Override
        void doOperation(Request request) {
            doSemaphoreOperation(request, (DistributedSemaphore) request.record.getValue());
        }

        @Override
        protected void onNoTimeToSchedule(Request request) {
            doResponse(request, null, SemaphoreProxy.ACQUIRE_FAILED, false);
            returnResponse(request);
        }

        protected void doResponse(Request request, DistributedSemaphore semaphore, long retValue, boolean changed) {
            final boolean wasScheduled = request.scheduled;
            final Record record = request.record;
            final List<ScheduledAction> scheduledActions = record.getScheduledActions();
            request.clearForResponse();
            if (changed) {
                record.setValueData(toData(semaphore));
                record.incrementVersion();
                request.version = record.getVersion();
                request.response = record.getValueData();
            }
            request.longValue = retValue;
            returnResponse(request);
            if (!wasScheduled && scheduledActions != null) {
                int remaining = scheduledActions.size();
                while (remaining-- > 0 && semaphore.getAvailable() > 0) {
                    ScheduledAction sa = scheduledActions.remove(0);
                    node.clusterManager.deregisterScheduledAction(sa);
                    if (!sa.expired()) {
                        sa.consume();
                    } else {
                        sa.onExpire();
                    }
                }
            }
        }
    }

    class SemaphoreAttachDetachOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final int permitsDelta = (int) request.longValue;
            semaphore.attachDetach(permitsDelta, request.caller);
            doResponse(request, semaphore, 0L, true);
        }
    }

    class SemaphoreCancelAcquireOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            long retValue = 0L;
            final List<ScheduledAction> scheduledActions = request.record.getScheduledActions();
            if (scheduledActions != null) {
                final int threadId = ThreadContext.get().getThreadId();
                final Iterator<ScheduledAction> i = scheduledActions.iterator();
                while (i.hasNext()) {
                    final ScheduledAction sa = i.next();
                    final Request sr = sa.getRequest();
                    if (sr.lockThreadId == threadId && sr.caller.equals(request.caller)) {
                        node.clusterManager.deregisterScheduledAction(sa);
                        doResponse(sr, null, SemaphoreProxy.ACQUIRE_FAILED, false);
                        i.remove();
                        retValue = 1L;
                        break;
                    }
                }
            }
            request.clearForResponse();
            request.longValue = retValue;
            returnResponse(request);
        }
    }

    class SemaphoreDestroyOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final List<ScheduledAction> scheduledActions = request.record.getScheduledActions();
            if (scheduledActions != null) {
                for (ScheduledAction sa : scheduledActions) {
                    final Request sr = sa.getRequest();
                    if (sr.caller.equals(request.caller) && sr.lockThreadId == ThreadContext.get().getThreadId()) {
                        node.clusterManager.deregisterScheduledAction(sa);
                        doResponse(sr, null, SemaphoreProxy.INSTANCE_DESTROYED, false);
                    }
                }
            }
            request.clearForResponse();
            returnResponse(request);
        }
    }

    class SemaphoreDrainOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final int drainedPermits = semaphore.drain();
            doResponse(request, semaphore, drainedPermits, drainedPermits > 0);
        }
    }

    class SemaphoreGetAttachedOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            doResponse(request, semaphore, semaphore.getAttached(request.caller), false);
        }
    }

    class SemaphoreGetAvailableOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            doResponse(request, semaphore, semaphore.getAvailable(), false);
        }
    }

    class SemaphoreReduceOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final int permits = (int) request.longValue;
            semaphore.reduce(permits);
            doResponse(request, semaphore, 0L, permits > 0);
        }
    }

    class SemaphoreReleaseOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final int permits = (int) request.longValue;
            final boolean detach = SemaphoreProxy.DATA_TRUE.equals(request.value);
            final Address detachAddress = detach ? request.caller : null;
            semaphore.release(permits, detachAddress);
            doResponse(request, semaphore, 0L, true);
        }
    }

    class SemaphoreTryAcquireOperationHandler extends SemaphoreOperationHandler {
        void doSemaphoreOperation(Request request, DistributedSemaphore semaphore) {
            final int permits = (int) request.longValue;
            final Boolean attach = SemaphoreProxy.DATA_TRUE.equals(request.value);
            final Address attachAddress = attach ? request.caller : null;
            if (semaphore.tryAcquire(permits, attachAddress)) {
                doResponse(request, semaphore, SemaphoreProxy.ACQUIRED, true);
            } else {
                request.lockThreadId = ThreadContext.get().getThreadId();
                schedule(request);
            }
        }
    }

    class AddOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.add(request, false);
        }
    }

    class EvictOperationHandler extends MTargetAwareOperationHandler {
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
                    storeExecutor.execute(new EvictStorer(cmap, request), request.key.hashCode());
                } else {
                    doOperation(request);
                    returnResponse(request);
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        void doOperation(Request request) {
            if (!testLock(request)) {
                request.response = Boolean.FALSE;
            } else {
                CMap cmap = getOrCreateMap(request.name);
                request.response = cmap.evict(request);
            }
        }

        class EvictStorer extends AbstractMapStoreOperation {

            EvictStorer(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object key = toObject(request.key);
                Object value = toObject(request.value);
                cmap.store.store(key, value);
                afterMapStore();
            }

            public void process() {
                if (success) doOperation(request);
                returnResponse(request);
            }
        }
    }

    class MergeOperationHandler extends MTargetAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request);
                boolean doesNotExist = (
                        record == null
                                || !record.isActive()
                                || !record.isValid()
                                || !record.hasValueData());
                DataRecordEntry existing = (doesNotExist) ? null : new DataRecordEntry(record);
                node.executorManager.executeNow(new MergeLoader(cmap, request, existing));
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        void doOperation(Request request) {
        }

        class MergeLoader extends AbstractMapStoreOperation {

            private DataRecordEntry existingRecord;

            MergeLoader(CMap cmap, Request request, DataRecordEntry existingRecord) {
                super(cmap, request);
                this.existingRecord = existingRecord;
            }

            @Override
            void doMapStoreOperation() {
                Object winner = null;
                success = false;
                if (cmap.mergePolicy != null) {
                    if (existingRecord == null && cmap.loader != null) {
                        existingRecord = new MGetDataRecordEntry().get(request.name, request.key);
                    }
                    DataRecordEntry newEntry = (DataRecordEntry) toObject(request.value);
                    Object key = newEntry.getKey();
                    if (key != null && newEntry.hasValue()) {
                        winner = cmap.mergePolicy.merge(cmap.getName(), newEntry, existingRecord);
                        if (winner != null) {
                            if (cmap.isMultiMap()) {
                                
                                if (winner == MergePolicy.REMOVE_EXISTING) {
                                    // TODO remove logging message if implementation is correct!
                                    logger.log(Level.FINER, String.format("Merge policy %s decided to REMOVE_EXISTING for key %s in map %s!", cmap.mergePolicy.getClass().getSimpleName(), cmap.getName(), key));
                                    MRemoveMulti mremove = node.concurrentMapManager.new MRemoveMulti();
                                    mremove.remove(request.name, request.key);
                                } else {
                                    MPutMulti mput = node.concurrentMapManager.new MPutMulti();
                                    mput.put(request.name, request.key, winner);
                                }
                            } else {
                                
                                if (winner == MergePolicy.REMOVE_EXISTING) {
                                    // TODO remove logging message if implementation is correct!
                                    logger.log(Level.FINER, String.format("Merge policy %s decided to REMOVE_EXISTING for key %s in map %s!", cmap.mergePolicy.getClass().getSimpleName(), cmap.getName(), key));
                                    MRemove mremove = node.concurrentMapManager.new MRemove();
                                    mremove.remove(request.name, request.key);
                                } else {
                                    ConcurrentMapManager.MPut mput = node.concurrentMapManager.new MPut();
                                    mput.put(request.name, request.key, winner, -1);                                    
                                }
                            }
                            success = true;
                        }
                    }
                }
            }

            public void process() {
                request.response = (success) ? Boolean.TRUE : Boolean.FALSE;
                returnResponse(request);
            }
        }
    }

    class GetMapEntryOperationHandler extends MTargetAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (cmap.loader != null && (record == null || record.isLoadable())) {
                storeExecutor.execute(new GetMapEntryLoader(cmap, request), request.key.hashCode());
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.getMapEntry(request);
        }

        class GetMapEntryLoader extends AbstractMapStoreOperation {

            GetMapEntryLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value = cmap.loader.load(toObject(request.key));
                if (value != null) {
                    setIndexValues(request, value);
                    request.value = toData(value);
                    putFromLoad(request);
                } else {
                    success = false;
                }
            }

            public void process() {
                if (success) {
                    Record record = cmap.createNewTransientRecord(request.key, request.value);
                    request.response = new CMap.CMapEntry(record);
                } else {
                    request.response = null;
                }
                returnResponse(request);
            }
        }
    }

    class GetDataRecordEntryOperationHandler extends MTargetAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request);
                if (cmap.loader != null && (record == null || record.isLoadable())) {
                    storeExecutor.execute(new GetDataRecordEntryLoader(cmap, request), request.key.hashCode());
                } else {
                    doOperation(request);
                    returnResponse(request);
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request.key);
            request.response = (record == null) ? null : new DataRecordEntry(record);
        }

        class GetDataRecordEntryLoader extends AbstractMapStoreOperation {

            GetDataRecordEntryLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value = cmap.loader.load(toObject(request.key));
                if (value != null) {
                    setIndexValues(request, value);
                    request.value = toData(value);
                } else {
                    success = false;
                }
            }

            public void process() {
                if (success) {
                    Record record = cmap.createNewTransientRecord(request.key, request.value);
                    record.setIndexes(request.indexes, request.indexTypes);
                    request.response = new DataRecordEntry(record);
                } else {
                    request.response = null;
                }
                returnResponse(request);
            }
        }
    }

    class GetOperationHandler extends MTargetAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (cmap.loader != null && (record == null || record.isLoadable())) {
                storeExecutor.execute(new GetLoader(cmap, request), request.key.hashCode());
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

        class GetLoader extends AbstractMapStoreOperation {

            GetLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value = cmap.loader.load(toObject(request.key));
                if (value != null) {
                    setIndexValues(request, value);
                    request.value = toData(value);
                    putFromLoad(request);
                } else {
                    success = false;
                }
            }

            public void process() {
                if (success) request.response = request.value;
                returnResponse(request);
            }
        }
    }

    class ContainsKeyOperationHandler extends MTargetAwareOperationHandler {
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request);
                if (cmap.loader != null && (record == null || record.isLoadable())) {
                    storeExecutor.execute(new ContainsKeyLoader(cmap, request), request.key.hashCode());
                } else {
                    doOperation(request);
                    returnResponse(request);
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.containsKey(request);
        }

        class ContainsKeyLoader extends AbstractMapStoreOperation {

            ContainsKeyLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value = cmap.loader.load(toObject(request.key));
                if (value != null) {
                    setIndexValues(request, value);
                    request.value = toData(value);
                    putFromLoad(request);
                } else {
                    success = false;
                }
            }

            public void process() {
                request.response = (success) ? Boolean.TRUE : Boolean.FALSE;
                returnResponse(request);
            }
        }
    }

    abstract class AbstractMapStoreOperation implements Runnable, Processable {
        final protected CMap cmap;
        final protected Request request;
        protected boolean success = true;

        protected AbstractMapStoreOperation(CMap cmap, Request request) {
            this.cmap = cmap;
            this.request = request;
        }

        public void run() {
            try {
                doMapStoreOperation();
            } catch (Exception e) {
                success = false;
                if (e instanceof ClassCastException) {
                    CMap cmap = getMap(request.name);
                    if (cmap.isMapForQueue() && e.getMessage().contains("java.lang.Long cannot be")) {
                        logger.log(Level.SEVERE, "This is MapStore for Queue. Make sure you treat the key as Long");
                    }
                }
                logger.log(Level.WARNING, "Store thrown exception for " + request.operation, e);
                request.response = toData(new AddressAwareException(e, thisAddress));
            } finally {
                enqueueAndReturn(AbstractMapStoreOperation.this);
            }
        }

        abstract void doMapStoreOperation();

        protected final void afterMapStore() {
            Record storedRecord = cmap.getRecord(request);
            if (storedRecord != null) {
                storedRecord.setLastStoredTime(Clock.currentTimeMillis());
                storedRecord.setDirty(false);
            }
        }
    }

    class ValueCountOperationHandler extends MTargetAwareOperationHandler {
        void doOperation(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            request.response = cmap.valueCount(request.key);
        }
    }

    class UnlockOperationHandler extends SchedulableOperationHandler {
        protected boolean shouldSchedule(Request request) {
            return false;
        }

        void doOperation(Request request) {
            boolean unlocked = true;
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (record != null) {
                unlocked = record.unlock(request.lockThreadId, request.lockAddress);
                if (unlocked) {
                    record.incrementVersion();
                    request.version = record.getVersion();
                    request.lockCount = record.getLockCount();
                    if (record.valueCount() == 0 && record.isEvictable()) {
                        cmap.markAsEvicted(record);
                    }
                    cmap.fireScheduledActions(record);
                }
            }
            if (unlocked) {
                request.response = Boolean.TRUE;
            } else {
                request.response = Boolean.FALSE;
            }
        }
    }

    class ForceUnlockOperationHandler extends SchedulableOperationHandler {
        protected boolean shouldSchedule(Request request) {
            return false;
        }

        void doOperation(Request request) {
            boolean unlocked = false;
            CMap cmap = getOrCreateMap(request.name);
            Record record = cmap.getRecord(request);
            if (record != null) {
                DistributedLock lock = record.getLock();
                if (lock != null && lock.getLockCount() > 0) {
                    record.clearLock();
                    unlocked = true;
                    record.incrementVersion();
                    request.version = record.getVersion();
                    request.lockCount = 0;
                    if (record.valueCount() == 0 &&
                            !record.hasScheduledAction()) {
                        cmap.markAsEvicted(record);
                    }
                    cmap.fireScheduledActions(record);
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
            request.response = -1L;
            returnResponse(request);
        }

        public void handle(Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                if (shouldSchedule(request)) {
                    if (request.hasEnoughTimeToSchedule()) {
                        if (systemLogService.shouldLog(INFO)) {
                            systemLogService.info(request,
                                    MapSystemLogFactory.newScheduleRequest(request, cmap.getRecord(request)));
                        }
                        schedule(request);
                    } else {
                        if (systemLogService.shouldLog(INFO)) {
                            systemLogService.info(request, "NoTimeToSchedule", request.name, request.operation);
                        }
                        onNoTimeToSchedule(request);
                    }
                } else {
                    final Record record = cmap.getRecord(request.key);
                    if (request.operation == CONCURRENT_MAP_TRY_LOCK_AND_GET
                            && cmap.loader != null && (record == null || !record.hasValueData())) {
                        storeExecutor.execute(new TryLockAndGetLoader(cmap, request), request.key.hashCode());
                    } else if (cmap.isMultiMap() && request.value != null) {
                        Collection<ValueHolder> col = record.getMultiValues();
                        if (col != null && col.size() > 0) {
                            storeExecutor.execute(new MultiMapContainsTask(request, col), request.key.hashCode());
                        } else {
                            doOperation(request);
                            returnResponse(request);
                        }
                    } else {
                        doOperation(request);
                        returnResponse(request);
                    }
                }
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

        class MultiMapContainsTask implements Runnable, Processable {
            private final Request request;
            private final Collection<ValueHolder> values;

            MultiMapContainsTask(Request request, Collection<ValueHolder> values) {
                this.request = request;
                this.values = values;
            }

            public void run() {
                if (!values.contains(new ValueHolder(request.value))) {
                    request.value = null;
                }
                enqueueAndReturn(MultiMapContainsTask.this);
            }

            public void process() {
                doOperation(request);
                returnResponse(request);
            }
        }

        class TryLockAndGetLoader extends AbstractMapStoreOperation {
            Data valueData = null;

            TryLockAndGetLoader(CMap cmap, Request request) {
                super(cmap, request);
            }

            @Override
            void doMapStoreOperation() {
                Object value = cmap.loader.load(toObject(request.key));
                valueData = toData(value);
            }

            public void process() {
                final Record record = cmap.getRecord(request);
                if (record != null && !record.testLock(request.lockThreadId, request.lockAddress)) {
                    // record is locked by a previous TryLockAndGetLoader operation
                    // return redo response.
                    returnRedoResponse(request, REDO_MAP_LOCKED);
                } else {
                    if (valueData != null) {
                        if (record == null) {
                            cmap.createAndAddNewRecord(request.key, valueData);
                        } else {
                            record.setValueData(valueData);
                        }
                    }
                    doOperation(request);
                    request.value = valueData;
                    returnResponse(request);
                }
            }
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
                returnRedoResponse(request, REDO_PARTITION_MIGRATING);
            }
        };
        record.addScheduledAction(scheduledAction);
        node.clusterManager.registerScheduledAction(scheduledAction);
    }

    class IsKeyLockedOperationHandler extends MTargetAwareOperationHandler {

        @Override
        void doOperation(Request request) {
            final CMap cmap = getOrCreateMap(request.name);
            if (cmap.isNotLocked(request)) {
                Record record = cmap.getRecord(request.key);
                request.response = record != null && record.isLocked();
                returnResponse(request);
            } else {
                returnRedoResponse(request, REDO_MAP_LOCKED);
            }
        }

    }

    abstract class SchedulableOperationHandler extends MTargetAwareOperationHandler {

        protected boolean shouldSchedule(Request request) {
            return (!testLock(request));
        }

        protected void onNoTimeToSchedule(Request request) {
            if (request.local) {
                request.response = distributedTimeoutException;
            } else {
                request.response = dataTimeoutException;
            }
            returnResponse(request);
        }

        protected void schedule(Request request) {
            scheduleRequest(SchedulableOperationHandler.this, request);
        }

        public void handle(Request request) {
            boolean shouldSchedule = shouldSchedule(request);
            SystemLogService css = systemLogService;
            if (css.shouldLog(TRACE)) {
                css.logObject(request, TRACE, "ShouldSchedule ");
            }
            if (shouldSchedule) {
                if (request.hasEnoughTimeToSchedule()) {
                    if (css.shouldLog(INFO)) {
                        css.info(request,
                                MapSystemLogFactory.newScheduleRequest(request, recordExist(request)));
                    }
                    schedule(request);
                } else {
                    if (css.shouldLog(INFO)) {
                        css.info(request, "NoTimeToSchedule", request.name, request.operation);
                    }
                    onNoTimeToSchedule(request);
                }
            } else {
                doOperation(request);
                returnResponse(request);
            }
        }
    }

    class ContainsEntryOperationHandler extends ResponsiveOperationHandler {

        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            final boolean isMigrating = isMigrating(request);
            if (cmap.isNotLocked(request) && !isMigrating) {
                Record record = cmap.getRecord(request);
                if (record == null || !record.isActive() || !record.isValid()) {
                    request.response = Boolean.FALSE;
                    returnResponse(request);
                } else {
                    node.executorManager.executeQueryTask(new ContainsEntryTask(request, record));
                }
            } else {
                returnRedoResponse(request, isMigrating ? REDO_PARTITION_MIGRATING : REDO_MAP_LOCKED);
            }
        }

        class ContainsEntryTask implements Runnable {
            final Request request;
            final Record record;

            ContainsEntryTask(Request request, Record record) {
                this.request = request;
                this.record = record;
            }

            public void run() {
                CMap cmap = getMap(request.name);
                Data value = request.value;
                request.response = Boolean.FALSE;
                if (cmap.isMultiMap()) {
                    Collection<ValueHolder> multiValues = record.getMultiValues();
                    if (multiValues != null) {
                        ValueHolder theValueHolder = new ValueHolder(value);
                        request.response = multiValues.contains(theValueHolder);
                    }
                } else {
                    Object obj = toObject(value);
                    request.response = obj.equals(record.getValue());
                }
                returnResponse(request);
            }
        }
    }

    class ContainsValueOperationHandler extends MigrationAwareExecutedOperationHandler {

        @Override
        Runnable createRunnable(final Request request) {
            return new ContainsValueTask(request);
        }

        class ContainsValueTask implements Runnable {
            final Request request;

            ContainsValueTask(Request request) {
                this.request = request;
            }

            public void run() {
                CMap cmap = getMap(request.name);
                Data value = request.value;
                request.response = Boolean.FALSE;
                if (cmap != null) {
                    MapIndexService mapIndexService = cmap.getMapIndexService();
                    long now = Clock.currentTimeMillis();
                    if (cmap.isMultiMap()) {
                        Collection<Record> records = mapIndexService.getOwnedRecords();
                        ValueHolder theValueHolder = new ValueHolder(value);
                        for (Record record : records) {
                            if (record.isActive() && record.isValid(now)) {
                                Collection<ValueHolder> multiValues = record.getMultiValues();
                                if (multiValues != null) {
                                    if (multiValues.contains(theValueHolder)) {
                                        request.response = Boolean.TRUE;
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        Collection<? extends MapEntry> results = null;
                        Index index = mapIndexService.getValueIndex();
                        if (index != null) {
                            results = index.getRecords((long) value.hashCode());
                        } else {
                            results = mapIndexService.getOwnedRecords();
                        }
                        if (results != null) {
                            Object obj = toObject(value);
                            for (MapEntry entry : results) {
                                Record record = (Record) entry;
                                if (record.isActive() && record.isValid(now)) {
                                    if (obj.equals(record.getValue())) {
                                        request.response = Boolean.TRUE;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                returnResponse(request);
            }
        }
    }

    abstract class ExecutedOperationHandler extends ResponsiveOperationHandler {
        public void process(Packet packet) {
            Request request = Request.copyFromPacket(packet);
            request.local = false;
            handle(request);
        }

        public void handle(final Request request) {
            node.executorManager.executeQueryTask(createRunnable(request));
        }

        abstract Runnable createRunnable(Request request);
    }

    abstract class MigrationAwareExecutedOperationHandler extends ExecutedOperationHandler {
        @Override
        public void handle(Request request) {
            CMap cmap = getOrCreateMap(request.name);
            final boolean isMigrating = isMigrating(request);
            if (cmap.isNotLocked(request) && !isMigrating) {
                super.handle(request);
            } else {
                returnRedoResponse(request, isMigrating ? REDO_PARTITION_MIGRATING : REDO_MAP_LOCKED);
            }
        }
    }

    public Pairs queryMap(CMap cmap, ClusterOperation operation, Predicate predicate) throws QueryException {
        try {
            final QueryContext queryContext = new QueryContext(cmap.getName(), predicate, cmap.getMapIndexService());
            Set<MapEntry> results = cmap.getMapIndexService().doQuery(queryContext);
            boolean evaluateValues = (predicate != null && !queryContext.isStrong());
            return createResultPairs(operation, results, evaluateValues, predicate);
        } catch (Throwable e) {
            throw new QueryException(e);
        }
    }

    private Pairs createResultPairs(ClusterOperation operation, Collection<MapEntry> colRecords, boolean evaluateEntries, Predicate predicate) {
        Pairs pairs = new Pairs();
        if (colRecords != null) {
            long now = currentTimeMillis();
            for (MapEntry mapEntry : colRecords) {
                Record record = (Record) mapEntry;
                if (record.isActive() && record.isValid(now)) {
                    if (record.getKeyData() == null || record.getKeyData().size() == 0) {
                        throw new RuntimeException("Key cannot be null or zero-size: " + record.getKeyData());
                    }
                    boolean match = (!evaluateEntries) || predicate.apply(record);
                    if (match) {
                        boolean onlyKeys = (operation == CONCURRENT_MAP_ITERATE_KEYS_ALL ||
                                operation == CONCURRENT_MAP_ITERATE_KEYS);
                        Data key = record.getKeyData();
                        if (record.hasValueData()) {
                            Data value = (onlyKeys) ? null : record.getValueData();
                            pairs.addKeyValue(new KeyValue(key, value));
                        } else if (record.getMultiValues() != null) {
                            int size = record.getMultiValues().size();
                            if (size > 0) {
                                if (operation == CONCURRENT_MAP_ITERATE_KEYS) {
                                    pairs.addKeyValue(new KeyValue(key, null));
                                } else {
                                    Collection<ValueHolder> values = record.getMultiValues();
                                    for (ValueHolder valueHolder : values) {
                                        pairs.addKeyValue(new KeyValue(key, valueHolder.getData()));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return pairs;
    }

    Record recordExist(Request req) {
        CMap cmap = maps.get(req.name);
        if (cmap == null) {
            return null;
        }
        return cmap.getRecord(req);
    }

    Record ensureRecord(Request req) {
        return ensureRecord(req, req.value);
    }

    Record ensureRecord(Request req, Data defaultValue) {
        checkServiceThread();
        CMap cmap = getOrCreateMap(req.name);
        Record record = cmap.getRecord(req);
        if (record == null || !record.isActive() || !record.isValid()) {
            final Map<Address, Boolean> listeners = record != null ? record.getListeners() : null;
            record = cmap.createAndAddNewRecord(req.key, defaultValue);
            record.setMapListeners(listeners);
        }
        return record;
    }

    private boolean testLock(Request req) {
        Record record = recordExist(req);
        return record == null || record.testLock(req.lockThreadId, req.lockAddress);
    }
}
