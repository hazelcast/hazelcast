/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationparker.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.NonFragmentedServiceNamespace;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.ReplicaSyncEvent;
import com.hazelcast.internal.services.ServiceNamespaceAware;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingBackupOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The WaitSet is a effectively a set of operations waiting for some condition. For example it could be a set of lock-operations
 * that are waiting for a lock to come available.
 */
public class WaitSet implements LiveOperationsTracker, Iterable<WaitSetEntry> {

    private static final long TIMEOUT_UPPER_BOUND = 1500;

    private final Queue<WaitSetEntry> queue = new ConcurrentLinkedQueue<>();
    private final ILogger logger;
    private final NodeEngine nodeEngine;
    private final Map<WaitNotifyKey, WaitSet> waitSetMap;
    private final Queue<WaitSetEntry> delayQueue;

    public WaitSet(ILogger logger,
                   NodeEngine nodeEngine,
                   Map<WaitNotifyKey, WaitSet> waitSetMap,
                   Queue<WaitSetEntry> delayQueue) {
        this.nodeEngine = nodeEngine;
        this.logger = logger;
        this.waitSetMap = waitSetMap;
        this.delayQueue = delayQueue;
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (WaitSetEntry entry : queue) {
            // we need to read out the data from the BlockedOperation; not from the WaitSetEntry
            Operation operation = entry.getOperation();
            liveOperations.add(operation.getCallerAddress(), operation.getCallId());
        }
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    public void park(BlockingOperation op) {
        long timeout = op.getWaitTimeout();
        WaitSetEntry entry = new WaitSetEntry(queue, op);
        entry.setNodeEngine(nodeEngine);
        if (logger.isFinestEnabled()) {
            logger.finest("Parking on %s as #%d: %s", op.getWaitKey(), queue.size(), op);
        }
        queue.offer(entry);
        if (timeout > -1 && timeout < TIMEOUT_UPPER_BOUND) {
            delayQueue.offer(entry);
        }
    }

    // Runs in partition-thread, and therefor we can assume we have exclusive access to the WaitNotifyKey
    // (since each WaitNotifyKey is mapped to a single partition). So a park will not be concurrently
    // executed with an unpark for the same key.
    public void unpark(Notifier notifier, WaitNotifyKey key) {
        WaitSetEntry entry = queue.peek();
        while (entry != null) {
            Operation op = entry.getOperation();
            if (notifier == op) {
                throw new IllegalStateException("Found cyclic wait-notify! -> " + notifier);
            }
            if (entry.isValid()) {
                if (entry.isExpired()) {
                    // expired
                    entry.onExpire();
                } else if (entry.isCancelled()) {
                    entry.onCancel();
                } else {
                    if (entry.shouldWait()) {
                        return;
                    }
                    OperationService operationService = nodeEngine.getOperationService();
                    operationService.run(op);
                }
                entry.setValid(false);
            }
            // consume
            queue.poll();

            entry = queue.peek();

            // If parkQueue.peek() returns null, we should deregister this specific
            // key to avoid memory leak. By contract we know that park() and unpark()
            // cannot be called in parallel.
            // We can safely remove this queue from registration map here.
            if (entry == null) {
                waitSetMap.remove(key);
            }
        }
    }

    /**
     * Invalidates parked operations for the migrated partition and sends a {@link PartitionMigratingException} as a
     * response.
     * This is executed under partition migration lock!
     */
    @SuppressWarnings("checkstyle:cyclomaticcomplexity")
    void onPartitionMigrate(PartitionMigrationEvent event) {
        Iterator<WaitSetEntry> it = queue.iterator();
        int partitionId = event.getPartitionId();

        boolean backupStillNeeded = event.getMigrationEndpoint() == MigrationEndpoint.SOURCE
                // the partition was backup partition. owner should not have backups parked
                && event.getCurrentReplicaIndex() > 0
                // the partition remains as backup partition, note that newReplicaIndex cannot be 0 for SOURCE
                && event.getNewReplicaIndex() > 0;

        if (logger.isFinestEnabled() && it.hasNext()) {
            // the queue may become empty concurrently after initial check of it.hasNext()
            // need to reasonably handle such situation
            var headKey = Optional.ofNullable(queue.peek()).map(e -> ((BlockingOperation) e.getOperation()).getWaitKey())
                    .orElse(null);
            logger.finest("onPartitionMigrate processes parked operations for partitionId=%d "
                            + " for migration %s key=%s backup still needed %s",
                    partitionId, event, headKey, backupStillNeeded);
        }
        while (it.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            WaitSetEntry entry = it.next();
            if (!entry.isValid()) {
                continue;
            }

            Operation op = entry.getOperation();
            if (partitionId != op.getPartitionId()) {
                continue;
            }

            if (backupStillNeeded && op instanceof BlockingBackupOperation bbo
                    // sanity check
                    && op.getReplicaIndex() == event.getCurrentReplicaIndex()
                    // is it still needed?
                    && bbo.shouldKeepAfterMigration(event)) {
                logger.finest("onPartitionMigrate updates replica index of operation %s", op);
                op.setReplicaIndex(event.getNewReplicaIndex());
            } else {
                entry.setValid(false);
                if (op instanceof BackupOperation) {
                    // if this was a backup, it does not have to be retried because as part of migration the data
                    // was already sent.
                    // onExecutionFailure is _not_ invoked here.
                    // The partition was just migrated or is no longer needed. In those cases we should _not_ mark it as dirty.
                    // This is especially pronounced for the just migrated partition (DESTINATION).
                    //
                    // Second case when this is invoked is backup promotion to owner.
                    // Blocking backup operations cannot be reliably executed after promotion: usually just after promotion
                    // the partition will be marked as migrating (needed to restore requested backup count).
                    // This will reject backup operations due to PartitionMigratingException and there is no mechanism
                    // to retry them. After promotion there is no other owner which could be used to sync this replica with.
                    //
                    // If we are here after promotion, this may mean that some data is lost. However, it is very hard
                    // to observe it. Backup ACK has not yet been sent. However, isClusterSafe/isMemberSafe would report
                    // that the cluster/member is safe after the client invocation finished (after 5s backup ack timeout)
                    // even though there still would be some parked backups. This is an improbable scenario.
                    // Nevertheless, isClusterSafe/isMemberSafe is not fully reliable with blocking backups.
                    logger.fine("onPartitionMigrate invalidates backup operation %s", op);
                } else {
                    // if this is a regular operation, it should be retried as a response to PartitionMigratingException
                    // on a new owner.
                    logger.fine("onPartitionMigrate invalidates operation %s", op);
                    PartitionMigratingException pme = new PartitionMigratingException(nodeEngine.getThisAddress(),
                            partitionId, op.getClass().getName(), op.getServiceName());
                    op.sendResponse(pme);
                }
                it.remove();
            }
        }
    }

    public void onReplicaSync(ReplicaSyncEvent syncEvent) {
        Iterator<WaitSetEntry> it = queue.iterator();
        int partitionId = syncEvent.partitionId();

        if (logger.isFinestEnabled() && it.hasNext()) {
            // the queue may become empty concurrently after initial check of it.hasNext()
            // need to reasonably handle such situation
            var headKey = Optional.ofNullable(queue.peek()).map(e -> ((BlockingOperation) e.getOperation()).getWaitKey())
                    .orElse(null);
            logger.finest("onReplicaSync processes parked operations for partitionId=%d "
                            + " for sync %s key=%s",
                    partitionId, syncEvent, headKey);
        }
        while (it.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            WaitSetEntry entry = it.next();
            if (!entry.isValid()) {
                continue;
            }

            Operation op = entry.getOperation();
            if (partitionId != op.getPartitionId() || op.getReplicaIndex() != syncEvent.replicaIndex()) {
                continue;
            }

            if (namespaceMatches(syncEvent, op)) {
                assert op instanceof BackupOperation : "Unexpected parked operation on backup replica";
                logger.fine("onReplicaSync invalidates backup operation %s", op);
                entry.setValid(false);
                it.remove();
            }
        }
    }

    private static boolean namespaceMatches(ReplicaSyncEvent syncEvent, Operation op) {
        if (op instanceof ServiceNamespaceAware awareOp) {
            return syncEvent.namespace().equals(awareOp.getServiceNamespace());
        } else {
            return syncEvent.namespace() == NonFragmentedServiceNamespace.INSTANCE;
        }
    }

    public void onShutdown() {
        Object response = new HazelcastInstanceNotActiveException();
        Address thisAddress = nodeEngine.getThisAddress();
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }

            Operation op = entry.getOperation();
            // only for local invocations, remote ones will be expired via #onMemberLeft()
            if (thisAddress.equals(op.getCallerAddress())) {
                try {
                    OperationResponseHandler responseHandler = op.getOperationResponseHandler();
                    responseHandler.sendResponse(op, response);
                } catch (Exception e) {
                    logger.finest("While sending HazelcastInstanceNotActiveException response...", e);
                }
            }

            queue.clear();
        }
    }

    // invoked after member left
    public void invalidateAll(UUID callerUuid) {
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }
            Operation op = entry.getOperation();
            // do not invalidate waiting backup operations to have consistent primary and backup:
            // primary operation already executed
            if (callerUuid.equals(op.getCallerUuid()) && !(op instanceof BackupOperation)) {
                entry.setValid(false);
            }
        }
    }

    // invoked after client disconnected
    public void cancelAll(UUID callerUuid, Throwable cause) {
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }
            Operation op = entry.getOperation();
            // do not invalidate waiting backup operations to have consistent primary and backup:
            // primary operation already executed
            if (callerUuid.equals(op.getCallerUuid()) && !(op instanceof BackupOperation)) {
                entry.cancel(cause);
            }
        }
    }

    // invoked after data structure is destroyed
    public void cancelAll(String serviceName, Object objectId, Throwable cause) {
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }
            WaitNotifyKey wnk = entry.blockingOperation.getWaitKey();
            if (serviceName.equals(wnk.getServiceName())
                    && objectId.equals(wnk.getObjectName())) {
                entry.cancel(cause);
            }
        }
    }

    // just for testing
    WaitSetEntry find(Operation op) {
        for (WaitSetEntry entry : queue) {
            if (entry.op == op) {
                return entry;
            }
        }
        return null;
    }

    public int size() {
        return queue.size();
    }

    // for testing purposes only
    public int totalValidWaitingOperationCount() {
        int count = 0;
        for (WaitSetEntry entry : queue) {
            if (entry.valid) {
                count++;
            }
        }
        return count;
    }

    @Override
    public Iterator<WaitSetEntry> iterator() {
        return queue.iterator();
    }

    @Override
    public String toString() {
        return "WaitSet{"
                + "queue=" + queue
                + ", delayQueue=" + delayQueue
                + '}';
    }
}
