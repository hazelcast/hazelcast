/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.partition.PartitionInfo;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TODO: Things to resolve:
 * 1) what will happen to the scheduled ops when a partition migrates?
 * Should we force them to redo or auto-expire each op?
 * 2) when a member dies, each partition should get notified for proper cleanup.
 */
public class PartitionContainer {
    private final Config config;
    private final MapService mapService;
    final PartitionInfo partitionInfo;
    final ConcurrentMap<String, MapPartition> maps = new ConcurrentHashMap<String, MapPartition>(1000);
    final ConcurrentMap<String, TransactionLog> transactions = new ConcurrentHashMap<String, TransactionLog>(1000);
    final ConcurrentMap<ScheduledOperationKey, Queue<ScheduledOperation>> mapScheduledOperations
            = new ConcurrentHashMap<ScheduledOperationKey, Queue<ScheduledOperation>>(1000);
    final ConcurrentMap<Long, GenericBackupOperation> waitingBackupOps = new ConcurrentHashMap<Long, GenericBackupOperation>(1000);

    public PartitionContainer(Config config, final MapService mapService, final PartitionInfo partitionInfo) {
        this.config = config;
        this.mapService = mapService;
        this.partitionInfo = partitionInfo;
    }

    void onDeadAddress(Address deadAddress) {
        // invalidate scheduled operations of dead
        invalidateScheduledOpsForDeadCaller(deadAddress);
        // invalidate transactions of dead
        // invalidate locks owned by dead
    }

    MapConfig getMapConfig(String name) {
        return config.findMatchingMapConfig(name.substring(2));
    }

    public MapPartition getMapPartition(String name) {
        MapPartition mapPartition = maps.get(name);
        if (mapPartition == null) {
            mapPartition = new MapPartition(name, PartitionContainer.this);
            final MapPartition currentMapPartition = maps.putIfAbsent(name, mapPartition);
            mapPartition = currentMapPartition == null ? mapPartition : currentMapPartition;
        }
        return mapPartition;
    }

    public TransactionLog getTransactionLog(String txnId) {
        return transactions.get(txnId);
    }

    public void addTransactionLogItem(String txnId, TransactionLogItem logItem) {
        TransactionLog log = transactions.get(txnId);
        if (log == null) {
            log = new TransactionLog(txnId);
            transactions.put(txnId, log);
        }
        log.addLogItem(logItem);
    }

    public void putTransactionLog(String txnId, TransactionLog txnLog) {
        transactions.put(txnId, txnLog);
    }

    void rollback(String txnId) {
        transactions.remove(txnId);
    }

    void commit(String txnId) {
//        TransactionLog txnLog = transactions.get(txnId);
        TransactionLog txnLog = transactions.remove(txnId); // TODO: not sure?
        if (txnLog == null) return;
        for (TransactionLogItem txnLogItem : txnLog.changes.values()) {
            System.out.println(mapService.getNodeService().getThisAddress() + " pc.commit " + txnLogItem);
            MapPartition mapPartition = getMapPartition(txnLogItem.getName());
            Data key = txnLogItem.getKey();
            if (txnLogItem.isRemoved()) {
                mapPartition.records.remove(key);
            } else {
                Record record = mapPartition.records.get(key);
                if (record == null) {
                    record = new DefaultRecord(mapPartition.partitionInfo.getPartitionId(), key, txnLogItem.getValue(), -1, -1, mapService.nextId());
                    mapPartition.records.put(key, record);
                } else {
                    record.setValueData(txnLogItem.getValue());
                }
                record.setActive();
                record.setDirty(true);
            }
        }
    }

    void scheduleOp(LockAwareOperation op) {
        ScheduledOperationKey key = new ScheduledOperationKey(op.getName(), op.getKey());
        Queue<ScheduledOperation> qScheduledOps = mapScheduledOperations.get(key);
        if (qScheduledOps == null) {
            qScheduledOps = new LinkedList<ScheduledOperation>(); // TODO: qScheduledOps may need to be thread-safe
            mapScheduledOperations.put(key, qScheduledOps);
        }
        qScheduledOps.add(new ScheduledOperation(op));
    }

    /**
     * Should not be called from a schedulable operations as
     * we might be removing the lock object
     */
    void onUnlock(LockInfo lock, String name, Data key) {
        ScheduledOperationKey scheduledOperationKey = new ScheduledOperationKey(name, key);
        Queue<ScheduledOperation> scheduledOps = mapScheduledOperations.get(scheduledOperationKey);
        ScheduledOperation scheduledOp = scheduledOps.peek();
        LockAwareOperation op = scheduledOp.getOperation();
        while (!scheduledOp.isValid() || lock.testLock(op.getThreadId(), op.getCaller())) {
            scheduledOps.poll();
            if (scheduledOp.isValid()) {
                if (scheduledOp.expired()) {
                    op.onExpire();
                } else {
                    op.doOp();
                }
                scheduledOp.setValid(false);
            }
            scheduledOp = scheduledOps.peek();
            op = scheduledOp.getOperation();
        }
        if (scheduledOps.isEmpty()) {
            mapScheduledOperations.remove(scheduledOperationKey);
        }
        if (!lock.isLocked()) {
            getMapPartition(name).removeLock(key);
        }
    }

    /**
     * MapService should periodically invalidate the expired scheduled ops so that
     * waiting calls can be notified eventually. We will scan all scheduled ops
     * every second and notify callers of expired ops. In order to reduce the cost of
     * scanning, we should store them efficiently. That is why scheduled ops are
     * stored per partitions, not per map. (imagine having 1000 maps, we don't want
     * to loops all these maps every second)
     */
    void invalidateExpiredScheduledOps() {
        Iterator<Queue<ScheduledOperation>> it = mapScheduledOperations.values().iterator();
        while (it.hasNext()) {
            Queue<ScheduledOperation> qScheduledOps = it.next();
            Iterator<ScheduledOperation> itOps = qScheduledOps.iterator();
            while (itOps.hasNext()) {
                ScheduledOperation so = itOps.next();
                if (so.isValid() && so.expired()) {
                    so.getOperation().onExpire();
                    so.setValid(false);
                    itOps.remove();
                }
            }
            if (qScheduledOps.isEmpty()) {
                it.remove();
            }
        }
    }

    /**
     * When a member dies, all its scheduled ops has to be invalidated
     *
     * @param deadAddress
     */
    void invalidateScheduledOpsForDeadCaller(Address deadAddress) {
        Iterator<Queue<ScheduledOperation>> it = mapScheduledOperations.values().iterator();
        while (it.hasNext()) {
            Queue<ScheduledOperation> qScheduledOps = it.next();
            Iterator<ScheduledOperation> itOps = qScheduledOps.iterator();
            while (itOps.hasNext()) {
                ScheduledOperation so = itOps.next();
                if (deadAddress.equals(so.getOperation().getCaller())) {
                    itOps.remove();
                }
            }
            if (qScheduledOps.isEmpty()) {
                it.remove();
            }
        }
    }

    /**
     * Expires all scheduled ops for a given map.
     * This should be called when a map is destroyed.
     *
     * @param mapName name of the destroyed map
     */
    void expireScheduledOps(String mapName) {
        Iterator<ScheduledOperationKey> itKeys = mapScheduledOperations.keySet().iterator();
        while (itKeys.hasNext()) {
            ScheduledOperationKey key = itKeys.next();
            if (key.mapName.equals(mapName)) {
                Queue<ScheduledOperation> ops = mapScheduledOperations.get(key);
                if (ops != null) {
                    Iterator<ScheduledOperation> itOps = ops.iterator();
                    while (itOps.hasNext()) {
                        ScheduledOperation so = itOps.next();
                        if (so.isValid()) {
                            so.getOperation().onExpire();
                            so.setValid(false);
                        }
                        itOps.remove();
                    }
                    if (ops.isEmpty()) {
                        itKeys.remove();
                    }
                }
            }
        }
    }
}
