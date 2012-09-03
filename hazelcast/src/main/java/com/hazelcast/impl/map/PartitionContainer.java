/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.impl.DefaultRecord;
import com.hazelcast.impl.Record;
import com.hazelcast.impl.partition.PartitionInfo;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;

import java.util.*;

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
    final Map<String, MapPartition> maps = new HashMap<String, MapPartition>(1000);
    final Map<String, TransactionLog> transactions = new HashMap<String, TransactionLog>(1000);
    final Map<ScheduledOperationKey, Queue<ScheduledOperation>> mapScheduledOperations = new HashMap<ScheduledOperationKey, Queue<ScheduledOperation>>(1000);
    final Map<Long, GenericBackupOperation> waitingBackupOps = new HashMap<Long, GenericBackupOperation>(1000);
    long version = 0;

    public PartitionContainer(Config config, MapService mapService, PartitionInfo partitionInfo) {
        this.config = config;
        this.mapService = mapService;
        this.partitionInfo = partitionInfo;
    }

    long incrementAndGetVersion() {
        version++;
        return version;
    }

    void handleBackupOperation(GenericBackupOperation op) {
        if (op.version == version + 1) {
            op.backupAndReturn();
            version++;
            while (waitingBackupOps.size() > 0) {
                GenericBackupOperation backupOp = waitingBackupOps.remove(version + 1);
                if (backupOp != null) {
                    backupOp.doBackup();
                    version++;
                } else {
                    return;
                }
            }
        } else if (op.version <= version) {
            op.sendResponse();
        } else {
            waitingBackupOps.put(op.version, op);
            op.sendResponse();
        }
        if (waitingBackupOps.size() > 3) {
            Set<GenericBackupOperation> ops = new TreeSet<GenericBackupOperation>(waitingBackupOps.values());
            for (GenericBackupOperation backupOp : ops) {
                backupOp.doBackup();
                version = backupOp.version;
            }
            waitingBackupOps.clear();
        }
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
            maps.put(name, mapPartition);
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
        TransactionLog txnLog = transactions.get(txnId);
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
                    record = new DefaultRecord(null, mapPartition.partitionInfo.getPartitionId(), key, txnLogItem.getValue(), -1, -1, mapService.nextId());
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
            qScheduledOps = new LinkedList<ScheduledOperation>();
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
                    op.doRun();
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
                }
                if (ops.isEmpty()) {
                    itKeys.remove();
                }
            }
        }
    }
}
