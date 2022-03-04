/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.exception.PartitionMigratingException;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * The WaitSet is a effectively a set of operations waiting for some condition. For example it could be a set of lock-operations
 * that are waiting for a lock to come available.
 */
public class WaitSet implements LiveOperationsTracker, Iterable<WaitSetEntry> {

    private static final long TIMEOUT_UPPER_BOUND = 1500;

    private final Queue<WaitSetEntry> queue = new ConcurrentLinkedQueue<WaitSetEntry>();
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
     * Invalidates all parked operations for the migrated partition and sends a {@link PartitionMigratingException} as a
     * response.
     * Invoked on the migration destination. This is executed under partition migration lock!
     */
    void onPartitionMigrate(MigrationInfo migrationInfo) {
        Iterator<WaitSetEntry> it = queue.iterator();
        int partitionId = migrationInfo.getPartitionId();
        while (it.hasNext()) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            WaitSetEntry entry = it.next();
            if (!entry.isValid()) {
                continue;
            }

            Operation op = entry.getOperation();
            if (partitionId == op.getPartitionId()) {
                entry.setValid(false);
                PartitionMigratingException pme = new PartitionMigratingException(nodeEngine.getThisAddress(),
                        partitionId, op.getClass().getName(), op.getServiceName());
                op.sendResponse(pme);
                it.remove();
            }
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

    public void invalidateAll(UUID callerUuid) {
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }
            Operation op = entry.getOperation();
            if (callerUuid.equals(op.getCallerUuid())) {
                entry.setValid(false);
            }
        }
    }

    public void cancelAll(UUID callerUuid, Throwable cause) {
        for (WaitSetEntry entry : queue) {
            if (!entry.isValid()) {
                continue;
            }
            Operation op = entry.getOperation();
            if (callerUuid.equals(op.getCallerUuid())) {
                entry.cancel(cause);
            }
        }
    }

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
}
