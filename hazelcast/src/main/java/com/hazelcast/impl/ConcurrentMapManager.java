///*
// * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.hazelcast.impl;
//
//import com.hazelcast.config.MapConfig;
//import com.hazelcast.core.*;
//import com.hazelcast.impl.base.*;
//import com.hazelcast.impl.concurrentmap.*;
//import com.hazelcast.impl.executor.ParallelExecutor;
//import com.hazelcast.impl.monitor.LocalMapStatsImpl;
//import com.hazelcast.impl.partition.PartitionInfo;
//import com.hazelcast.impl.wan.WanMergeListener;
//import com.hazelcast.merge.MergePolicy;
//import com.hazelcast.nio.Address;
//import com.hazelcast.nio.Connection;
//import com.hazelcast.nio.Data;
//import com.hazelcast.nio.Packet;
//import com.hazelcast.partition.Partition;
//import com.hazelcast.query.Index;
//import com.hazelcast.query.MapIndexService;
//import com.hazelcast.query.Predicate;
//import com.hazelcast.query.QueryContext;
//import com.hazelcast.util.Clock;
//import com.hazelcast.util.DistributedTimeoutException;
//
//import java.util.*;
//import java.util.concurrent.*;
//import java.util.logging.Level;
//
//import static com.hazelcast.impl.ClusterOperation.*;
//import static com.hazelcast.impl.Constants.RedoType.*;
//import static com.hazelcast.impl.TransactionImpl.DEFAULT_TXN_TIMEOUT;
//import static com.hazelcast.impl.base.SystemLogService.Level.INFO;
//import static com.hazelcast.impl.base.SystemLogService.Level.TRACE;
//import static com.hazelcast.nio.IOUtil.toData;
//import static com.hazelcast.nio.IOUtil.toObject;
//import static com.hazelcast.util.Clock.currentTimeMillis;
//
//public class ConcurrentMapManager extends BaseManager {
//    private static final String BATCH_OPS_EXECUTOR_NAME = "hz.batch";
//
//    final int PARTITION_COUNT;
//    final int MAX_BACKUP_COUNT;
//    final long GLOBAL_REMOVE_DELAY_MILLIS;
//    final boolean LOG_STATE;
////    long lastLogStateTime = currentTimeMillis();
//    final ConcurrentMap<String, CMap> maps;
//    final ConcurrentMap<String, NearCache> mapCaches;
//    final PartitionServiceImpl partitionServiceImpl;
//    final PartitionManager partitionManager;
//    long newRecordId = 0;
////    final ParallelExecutor storeExecutor;
//    final Executor storeExecutor;
////    final ParallelExecutor evictionExecutor;
//    final Executor evictionExecutor;
//    final RecordFactory recordFactory;
//    final Collection<WanMergeListener> colWanMergeListeners = new CopyOnWriteArrayList<WanMergeListener>();
//
//    ConcurrentMapManager(final Node node) {
//        super(node);
//        recordFactory = node.initializer.getRecordFactory();
////        storeExecutor = node.executorManager.newParallelExecutor(node.groupProperties.EXECUTOR_STORE_THREAD_COUNT.getInteger());
////        evictionExecutor = node.executorManager.newParallelExecutor(node.groupProperties.EXECUTOR_STORE_THREAD_COUNT.getInteger());
//        storeExecutor = node.nodeService.getExecutorService();
//        evictionExecutor = node.nodeService.getExecutorService();
//        PARTITION_COUNT = node.groupProperties.CONCURRENT_MAP_PARTITION_COUNT.getInteger();
//        MAX_BACKUP_COUNT = MapConfig.MAX_BACKUP_COUNT;
//        int removeDelaySeconds = node.groupProperties.REMOVE_DELAY_SECONDS.getInteger();
//        if (removeDelaySeconds <= 0) {
//            logger.log(Level.WARNING, GroupProperties.PROP_REMOVE_DELAY_SECONDS
//                    + " must be greater than zero. Setting to 1.");
//            removeDelaySeconds = 1;
//        }
//        GLOBAL_REMOVE_DELAY_MILLIS = removeDelaySeconds * 1000L;
//        LOG_STATE = node.groupProperties.LOG_STATE.getBoolean();
//        maps = new ConcurrentHashMap<String, CMap>(10, 0.75f, 1);
//        mapCaches = new ConcurrentHashMap<String, NearCache>(10, 0.75f, 1);
//        partitionManager = node.partitionManager;
//        partitionServiceImpl = new PartitionServiceImpl(this);
//        node.nodeService.getScheduledExecutorService().scheduleAtFixedRate(new Runnable() {
//            public void run() {
//                startCleanup(true, false);
//            }
//        }, 1, 1, TimeUnit.SECONDS);
//        registerPacketProcessor(CONCURRENT_MAP_GET_MAP_ENTRY, new GetMapEntryOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_GET_DATA_RECORD_ENTRY, new GetDataRecordEntryOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_GET, new GetOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_ASYNC_MERGE, new AsyncMergePacketProcessor());
//        registerPacketProcessor(CONCURRENT_MAP_WAN_MERGE, new WanMergePacketProcessor());
//        registerPacketProcessor(CONCURRENT_MAP_MERGE, new MergeOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_TRY_PUT, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_SET, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_PUT, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_PUT_AND_UNLOCK, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_PUT_IF_ABSENT, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_NOT_NULL, new PutOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_PUT_TRANSIENT, new PutTransientOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REPLACE_IF_SAME, new ReplaceOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_PUT_MULTI, new PutMultiOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REMOVE, new RemoveOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_EVICT, new EvictOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REMOVE_IF_SAME, new RemoveIfSameOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REMOVE_ITEM, new RemoveItemOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_PUT_AND_UNLOCK, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_ADD, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE_MULTI, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_REMOVE, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_BACKUP_LOCK, new BackupOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_LOCK, new LockOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_IS_KEY_LOCKED, new IsKeyLockedOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_TRY_LOCK_AND_GET, new LockOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_UNLOCK, new UnlockOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_FORCE_UNLOCK, new ForceUnlockOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_LOCK_MAP, new LockMapOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_UNLOCK_MAP, new LockMapOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_REMOVE_MULTI, new RemoveMultiOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_LIST, new AddOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_ADD_TO_SET, new AddOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_KEY, new ContainsKeyOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_ENTRY, new ContainsEntryOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_CONTAINS_VALUE, new ContainsValueOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_VALUE_COUNT, new ValueCountOperationHandler());
//        registerPacketProcessor(CONCURRENT_MAP_INVALIDATE, new InvalidateOperationHandler());
////        registerPacketProcessor(ATOMIC_NUMBER_ADD_AND_GET, new AtomicNumberAddAndGetOperationHandler());
////        registerPacketProcessor(ATOMIC_NUMBER_COMPARE_AND_SET, new AtomicNumberCompareAndSetOperationHandler());
////        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_ADD, new AtomicNumberGetAndAddOperationHandler());
////        registerPacketProcessor(ATOMIC_NUMBER_GET_AND_SET, new AtomicNumberGetAndSetOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_AWAIT, new CountDownLatchAwaitOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_COUNT_DOWN, new CountDownLatchCountDownOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_DESTROY, new CountDownLatchDestroyOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_GET_COUNT, new CountDownLatchGetCountOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_GET_OWNER, new CountDownLatchGetOwnerOperationHandler());
////        registerPacketProcessor(COUNT_DOWN_LATCH_SET_COUNT, new CountDownLatchSetCountOperationHandler());
////        registerPacketProcessor(SEMAPHORE_ATTACH_DETACH_PERMITS, new SemaphoreAttachDetachOperationHandler());
////        registerPacketProcessor(SEMAPHORE_CANCEL_ACQUIRE, new SemaphoreCancelAcquireOperationHandler());
////        registerPacketProcessor(SEMAPHORE_DESTROY, new SemaphoreDestroyOperationHandler());
////        registerPacketProcessor(SEMAPHORE_DRAIN_PERMITS, new SemaphoreDrainOperationHandler());
////        registerPacketProcessor(SEMAPHORE_GET_ATTACHED_PERMITS, new SemaphoreGetAttachedOperationHandler());
////        registerPacketProcessor(SEMAPHORE_GET_AVAILABLE_PERMITS, new SemaphoreGetAvailableOperationHandler());
////        registerPacketProcessor(SEMAPHORE_REDUCE_PERMITS, new SemaphoreReduceOperationHandler());
////        registerPacketProcessor(SEMAPHORE_RELEASE, new SemaphoreReleaseOperationHandler());
////        registerPacketProcessor(SEMAPHORE_TRY_ACQUIRE, new SemaphoreTryAcquireOperationHandler());
//    }
//
//    public PartitionServiceImpl getPartitionServiceImpl() {
//        return partitionServiceImpl;
//    }
//
//    public boolean registerAndSend(Address target, Packet packet, Call call) {
//        packet.callId = localIdGen.incrementAndGet();
//        mapCalls.put(packet.callId, call);
//        Connection targetConnection = node.connectionManager.getOrConnect(target);
////        System.out.println("targetConnection = " + targetConnection);
//        return send(packet, targetConnection);
//    }
//
//    Address getOwner(Data key) {
//        int partitionId = getPartitionId(key);
//        MemberImpl member = (MemberImpl) partitionServiceImpl.getPartition(partitionId).getOwner();
//        if (member == null) {
//            return null;
//        }
//        return member.getAddress();
//    }
//
//    public PartitionManager getPartitionManager() {
//        return partitionManager;
//    }
//
//    public void addWanMergeListener(WanMergeListener listener) {
//        colWanMergeListeners.add(listener);
//    }
//
//    public void removeWanMergeListener(WanMergeListener listener) {
//        colWanMergeListeners.remove(listener);
//    }
//
//    public void onRestart() {
//        enqueueAndWait(new Processable() {
//            public void process() {
//                for (CMap cmap : maps.values()) {
//                    // do not invalidate records,
//                    // values will be invalidated after merge
//                    cmap.reset(false);
//                }
//            }
//        }, 5);
//    }
//
//    public void reset() {
//        maps.clear();
//        mapCaches.clear();
//    }
//
//    public void shutdown() {
//        for (CMap cmap : maps.values()) {
//            try {
//                logger.log(Level.FINEST, "Destroying CMap[" + cmap.name + "]");
//                flush(cmap.name);
//                cmap.destroy();
//            } catch (Throwable e) {
//                if (node.isActive()) {
//                    logger.log(Level.SEVERE, e.getMessage(), e);
//                }
//            }
//        }
//        reset();
//    }
//
//    public void flush(String name) {
//        CMap cmap = getMap(name);
//        if (cmap != null && cmap.store != null && cmap.writeDelayMillis > 0) {
//            final Set<Record> dirtyRecords = new HashSet<Record>();
//            for (Record record : cmap.mapRecords.values()) {
//                if (record.isDirty()) {
//                    PartitionInfo partition = partitionManager.getPartition(record.getBlockId());
//                    Address owner = partition.getOwner();
//                    if (owner != null && thisAddress.equals(owner)) {
//                        dirtyRecords.add(record);
//                        record.setDirty(false);  // set dirty to false, we will store these soon
//                    }
//                }
//            }
//            cmap.runStoreUpdate(dirtyRecords);
//        }
//    }
//
//    public void syncForDead(MemberImpl deadMember) {
////        syncForDeadSemaphores(deadMember.getAddress());
////        syncForDeadCountDownLatches(deadMember.getAddress());
////        partitionManager.syncForDead(deadMember);
//    }
//
////    void syncForDeadSemaphores(Address deadAddress) {
////        CMap cmap = maps.get(MapConfig.SEMAPHORE_MAP_NAME);
////        if (cmap != null) {
////            for (Record record : cmap.mapRecords.values()) {
////                DistributedSemaphore semaphore = (DistributedSemaphore) record.getValue();
////                if (semaphore.onDisconnect(deadAddress)) {
////                    record.setValueData(toData(semaphore));
////                    record.incrementVersion();
////                }
////            }
////        }
////    }
////
////    void syncForDeadCountDownLatches(Address deadAddress) {
////        final CMap cmap = maps.get(MapConfig.COUNT_DOWN_LATCH_MAP_NAME);
////        if (deadAddress != null && cmap != null) {
////            for (Record record : cmap.mapRecords.values()) {
////                DistributedCountDownLatch cdl = (DistributedCountDownLatch) record.getValue();
////                if (cdl != null && cdl.isOwnerOrMemberAddress(deadAddress)) {
////                    List<ScheduledAction> scheduledActions = record.getScheduledActions();
////                    if (scheduledActions != null) {
////                        for (ScheduledAction sa : scheduledActions) {
////                            node.clusterImpl.deregisterScheduledAction(sa);
////                            final Request sr = sa.getRequest();
////                            sr.clearForResponse();
////                            sr.lockAddress = deadAddress;
////                            sr.longValue = CountDownLatchProxy.OWNER_LEFT;
////                            returnResponse(sr);
////                        }
////                        scheduledActions.clear();
////                    }
////                    cdl.setOwnerLeft();
////                }
////            }
////        }
////    }
//
////    public void syncForAdd() {
////        partitionManager.syncForAdd();
////    }
//
////    void logState() {
////        long now = currentTimeMillis();
////        if (LOG_STATE && ((now - lastLogStateTime) > 15000)) {
////            StringBuffer sbState = new StringBuffer(thisAddress + " State[" + new Date(now));
////            sbState.append("]");
////            Collection<Call> calls = mapCalls.values();
////            sbState.append("\nCall Count:").append(calls.size());
////            sbState.append(partitionManager.toString());
////            Collection<CMap> cmaps = maps.values();
////            for (CMap cmap : cmaps) {
////                cmap.appendState(sbState);
////            }
////            CpuUtilization cpuUtilization = node.getCpuUtilization();
////            node.connectionManager.appendState(sbState);
////            node.executorManager.appendState(sbState);
////            node.clusterManager.appendState(sbState);
////            long total = Runtime.getRuntime().totalMemory();
////            long free = Runtime.getRuntime().freeMemory();
////            sbState.append("\nCluster Size:").append(node.getClusterImpl().getSize());
////            sbState.append("\n").append(cpuUtilization);
////            sbState.append("\nUsed Memory:");
////            sbState.append((total - free) / 1024 / 1024);
////            sbState.append("MB");
////            logger.log(Level.INFO, sbState.toString());
////            lastLogStateTime = now;
////        }
////    }
//
//    /**
//     * Should be called from ExecutorService threads.
//     *
//     * @param mergingEntry
//     */
//    public void mergeWanRecord(DataRecordEntry mergingEntry) {
//        String name = mergingEntry.getName();
//        DataRecordEntry existingEntry = new MGetDataRecordEntry().get(name, mergingEntry.getKeyData());
//        final CMap cmap = node.concurrentMapManager.getMap(name);
//        MProxy mproxy = (MProxy) node.factory.getOrCreateProxyByName(name);
//        MergePolicy mergePolicy = cmap.wanMergePolicy;
//        if (mergePolicy == null) {
//            logger.log(Level.SEVERE, "Received wan merge but no merge policy defined!");
//        } else {
//            Object winner = mergePolicy.merge(cmap.getName(), mergingEntry, existingEntry);
//            if (winner != null) {
//                if (winner == MergePolicy.REMOVE_EXISTING) {
//                    mproxy.removeForSync(mergingEntry.getKey());
//                    notifyWanMergeListeners(WanMergeListener.EventType.REMOVED);
//                } else {
//                    mproxy.putForSync(mergingEntry.getKeyData(), winner);
//                    notifyWanMergeListeners(WanMergeListener.EventType.UPDATED);
//                }
//            } else {
//                notifyWanMergeListeners(WanMergeListener.EventType.IGNORED);
//            }
//        }
//    }
//
//    void notifyWanMergeListeners(WanMergeListener.EventType eventType) {
//        for (WanMergeListener wanMergeListener : colWanMergeListeners) {
//            if (eventType == WanMergeListener.EventType.UPDATED) {
//                wanMergeListener.entryUpdated();
//            } else if (eventType == WanMergeListener.EventType.REMOVED) {
//                wanMergeListener.entryRemoved();
//            } else {
//                wanMergeListener.entryIgnored();
//            }
//        }
//    }
//
//    public int getPartitionCount() {
//        return PARTITION_COUNT;
//    }
//
//    public Map<String, CMap> getCMaps() {
//        return maps;
//    }
//
//    Object tryLockAndGet(String name, Object key, long timeout) throws TimeoutException {
//        try {
//            MLock mlock = new MLock();
//            boolean locked = mlock.lockAndGetValue(name, key, timeout);
//            if (!locked) {
//                throw new TimeoutException();
//            } else {
//                return toObject(mlock.oldValue);
//            }
//        } catch (OperationTimeoutException e) {
//            throw new TimeoutException();
//        }
//    }
//
//    void putAndUnlock(String name, Object key, Object value) {
//        ThreadContext tc = ThreadContext.get();
//        Data dataKey = toData(key);
//        CMap cmap = getMap(name);
//        final LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
//        final boolean shouldUnlock = localLock != null
//                && localLock.getThreadId() == tc.getThreadId();
//        final boolean shouldRemove = shouldUnlock && localLock.getCount() == 1;
//        MPut mput = new MPut();
//        if (shouldRemove) {
//            mput.txnalPut(CONCURRENT_MAP_PUT_AND_UNLOCK, name, key, value, -1, -1);
//            // remove if current LocalLock is not changed
//            cmap.mapLocalLocks.remove(dataKey, localLock);
//        } else if (shouldUnlock) {
//            mput.txnalPut(CONCURRENT_MAP_PUT, name, key, value, -1, -1);
//            localLock.decrementAndGet();
//        } else {
//            mput.clearRequest();
//            final String error = "Current thread is not owner of lock. putAndUnlock could not be completed! " +
//                    "Thread-Id: " + tc.getThreadId() + ", LocalLock: " + localLock;
//            logger.log(Level.WARNING, error);
//            throw new IllegalStateException(error);
//        }
//        mput.clearRequest();
//    }
//
//    public void destroyEndpointThreads(Address endpoint, Set<Integer> threadIds) {
//        node.clusterImpl.invalidateScheduledActionsFor(endpoint, threadIds);
//        for (CMap cmap : maps.values()) {
//            for (Record record : cmap.mapRecords.values()) {
//                DistributedLock lock = record.getLock();
//                if (lock != null && lock.isLocked()) {
//                    if (endpoint.equals(record.getLockAddress()) && threadIds.contains(record.getLock().getLockThreadId())) {
//                        record.clearLock();
//                        cmap.fireScheduledActions(record);
//                    }
//                }
//            }
//        }
//    }
//
//    public PartitionInfo getPartitionInfo(int partitionId) {
//        return partitionManager.getPartition(partitionId);
//    }
//
////    public void sendMigrationEvent(boolean started, MigratingPartition migratingPartition) {
////        sendProcessableToAll(new MigrationNotification(started, migratingPartition), true);
////    }
//
//    public void startCleanup(final boolean now, final boolean force) {
//        if (now) {
//            for (CMap cMap : maps.values()) {
//                cMap.startCleanup(force);
//            }
//        } else {
//            node.nodeService.getExecutorService().execute(new Runnable() {
//                public void run() {
//                    for (CMap cMap : maps.values()) {
//                        cMap.startCleanup(force);
//                    }
//                }
//            });
//        }
//    }
//
//    public void executeCleanup(final CMap cmap, final boolean force) {
//        node.nodeService.getExecutorService().execute(new Runnable() {
//            public void run() {
//                cmap.startCleanup(force);
//            }
//        });
//    }
//
//    public boolean lock(String name, Object key, long timeout) {
//        MLock mlock = new MLock();
//        final boolean booleanCall = timeout >= 0 ; // tryLock
//        try {
//            final boolean locked = mlock.lock(name, key, timeout);
//            if (!locked && !booleanCall) {
//                throw new OperationTimeoutException(mlock.request.operation.toString(),
//                        "Lock request is timed out! t: " + mlock.request.timeout);
//            }
//            return locked;
//        } catch (OperationTimeoutException e) {
//            if (!booleanCall) {
//                throw e;
//            } else {
//                return false;
//            }
//        }
//    }
//
//    class MLock extends MBackupAndMigrationAwareOp {
//        volatile Data oldValue = null;
//
//        public boolean unlock(String name, Object key, long timeout) {
//            Data dataKey = toData(key);
//            ThreadContext tc = ThreadContext.get();
//            CMap cmap = getMap(name);
//            if (cmap == null) return false;
//            LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
//            if (localLock != null && localLock.getThreadId() == tc.getThreadId()) {
//                if (localLock.decrementAndGet() > 0) return true;
//                boolean unlocked = booleanCall(CONCURRENT_MAP_UNLOCK, name, dataKey, null, timeout, -1);
//                // remove if current LocalLock is not changed
//                cmap.mapLocalLocks.remove(dataKey, localLock);
//                if (unlocked) {
//                    request.lockAddress = null;
//                    request.lockCount = 0;
//                    backup(CONCURRENT_MAP_BACKUP_LOCK);
//                }
//                return unlocked;
//            }
//            return false;
//        }
//
//        public boolean forceUnlock(String name, Object key) {
//            Data dataKey = toData(key);
//            boolean unlocked = booleanCall(CONCURRENT_MAP_FORCE_UNLOCK, name, dataKey, null, 0, -1);
//            if (unlocked) {
//                backup(CONCURRENT_MAP_BACKUP_LOCK);
//            }
//            return unlocked;
//        }
//
//        public boolean lock(String name, Object key, long timeout) {
//            return lock(CONCURRENT_MAP_LOCK, name, key, null, timeout);
//        }
//
//        public boolean lockAndGetValue(String name, Object key, long timeout) {
//            return lock(CONCURRENT_MAP_TRY_LOCK_AND_GET, name, key, null, timeout);
//        }
//
//        public boolean lockAndGetValue(String name, Object key, Object value, long timeout) {
//            return lock(CONCURRENT_MAP_TRY_LOCK_AND_GET, name, key, value, timeout);
//        }
//
//        public boolean lock(ClusterOperation op, String name, Object key, Object value, long timeout) {
//            Data dataKey = toData(key);
//            ThreadContext tc = ThreadContext.get();
//            setLocal(op, name, dataKey, value, timeout, -1);
//            request.setLongRequest();
//            doOp();
//            long result = (Long) getResultAsObject();
//            if (result == -1L) {
//                return false;
//            }
//            else {
//                CMap cmap = getMap(name);
//                if (result == 0) {
//                    cmap.mapLocalLocks.remove(dataKey);
//                }
//                LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
//                if (localLock == null || localLock.getThreadId() != tc.getThreadId()) {
//                    localLock = new LocalLock(tc.getThreadId());
//                    cmap.mapLocalLocks.put(dataKey, localLock);
//                }
//                if (localLock.incrementAndGet() == 1) {
//                    backup(CONCURRENT_MAP_BACKUP_LOCK);
//                }
//            }
//            return true;
//        }
//
//        public boolean isLocked(String name, Object key) {
//            Data dataKey = toData(key);
//            CMap cmap = getMap(name);
//            if (cmap != null) {
//                LocalLock localLock = cmap.mapLocalLocks.get(dataKey);
//                if (localLock != null && localLock.getCount() > 0) {
//                    return true;
//                }
//            }
//            setLocal(CONCURRENT_MAP_IS_KEY_LOCKED, name, dataKey, null, -1, -1);
//            request.setBooleanRequest();
//            doOp();
//            return (Boolean) getResultAsObject();
//        }
//
