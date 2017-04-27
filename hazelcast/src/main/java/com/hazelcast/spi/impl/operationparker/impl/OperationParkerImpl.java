/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.LiveOperations;
import com.hazelcast.spi.LiveOperationsTracker;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.OperationParker;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OperationParkerImpl implements OperationParker, LiveOperationsTracker, MetricsProvider {

    private static final long FIRST_WAIT_TIME = 1000;
    private static final long TIMEOUT_UPPER_BOUND = 1500;

    private final ConcurrentMap<WaitNotifyKey, Queue<ParkedOperation>> parkQueueMap =
            new ConcurrentHashMap<WaitNotifyKey, Queue<ParkedOperation>>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationExecutor;
    private final Future expirationTaskFuture;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final ConstructorFunction<WaitNotifyKey, Queue<ParkedOperation>> parkQueueConstructor
            = new ConstructorFunction<WaitNotifyKey, Queue<ParkedOperation>>() {
        @Override
        public Queue<ParkedOperation> createNew(WaitNotifyKey key) {
            return new ConcurrentLinkedQueue<ParkedOperation>();
        }
    };

    public OperationParkerImpl(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        Node node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationParker.class.getName());

        this.expirationExecutor = Executors.newSingleThreadExecutor(
                new SingleExecutorThreadFactory(node.getConfigClassLoader(),
                        createThreadName(nodeEngine.getHazelcastInstance().getName(), "operation-parker")));

        this.expirationTaskFuture = expirationExecutor.submit(new ExpirationTask());
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        nodeEngine.getMetricsRegistry().scanAndRegister(this, "operation-parker");
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            for (ParkedOperation parkedOperation : parkQueue) {
                // we need to read out the data from the BlockedOperation; not from the ParkerOperation-container.
                Operation operation = parkedOperation.getOperation();
                liveOperations.add(operation.getCallerAddress(), operation.getCallId());
            }
        }
    }

    private void invalidate(ParkedOperation parkedOperation) throws Exception {
        nodeEngine.getOperationService().execute(parkedOperation);
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void park(BlockingOperation op) {
        final WaitNotifyKey key = op.getWaitKey();
        final Queue<ParkedOperation> parkQueue = getOrPutIfAbsent(parkQueueMap, key, parkQueueConstructor);
        long timeout = op.getWaitTimeout();
        ParkedOperation parkedOperation = new ParkedOperation(parkQueue, op);
        parkedOperation.setNodeEngine(nodeEngine);
        parkQueue.offer(parkedOperation);
        if (timeout > -1 && timeout < TIMEOUT_UPPER_BOUND) {
            delayQueue.offer(parkedOperation);
        }
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void unpark(Notifier notifier) {
        WaitNotifyKey key = notifier.getNotifiedKey();
        Queue<ParkedOperation> parkQueue = parkQueueMap.get(key);
        if (parkQueue == null) {
            return;
        }
        ParkedOperation parkedOp = parkQueue.peek();
        while (parkedOp != null) {
            Operation op = parkedOp.getOperation();
            if (notifier == op) {
                throw new IllegalStateException("Found cyclic wait-notify! -> " + notifier);
            }
            if (parkedOp.isValid()) {
                if (parkedOp.isExpired()) {
                    // expired
                    parkedOp.onExpire();
                } else {
                    if (parkedOp.shouldWait()) {
                        return;
                    }
                    nodeEngine.getOperationService().run(op);
                }
                parkedOp.setValid(false);
            }
            // consume
            parkQueue.poll();

            parkedOp = parkQueue.peek();

            // If parkQueue.peek() returns null, we should deregister this specific
            // key to avoid memory leak. By contract we know that park() and unpark()
            // cannot be called in parallel.
            // We can safely remove this queue from registration map here.
            if (parkedOp == null) {
                parkQueueMap.remove(key);
            }
        }
    }

    @Probe
    public int getParkQueueCount() {
        return parkQueueMap.size();
    }

    @Probe
    public int getTotalParkedOperationCount() {
        int count = 0;
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            count += parkQueue.size();
        }
        return count;
    }

    // for testing purposes only
    public int getTotalValidWaitingOperationCount() {
        int count = 0;
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            for (ParkedOperation parkedOperation : parkQueue) {
                if (parkedOperation.valid) {
                    count++;
                }
            }
        }
        return count;
    }

    // invalidated waiting ops will removed from queue eventually by notifiers.
    public void onMemberLeft(MemberImpl leftMember) {
        invalidateWaitingOps(leftMember.getUuid());
    }

    public void onClientDisconnected(String clientUuid) {
        invalidateWaitingOps(clientUuid);
    }

    private void invalidateWaitingOps(String callerUuid) {
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            for (ParkedOperation parkedOperation : parkQueue) {
                if (!parkedOperation.isValid()) {
                    continue;
                }
                Operation op = parkedOperation.getOperation();
                if (callerUuid.equals(op.getCallerUuid())) {
                    parkedOperation.setValid(false);
                }
            }
        }
    }

    /**
     * Invalidates all parked operations for the migrated partition and sends a {@link PartitionMigratingException} as a
     * response.
     * Invoked on the migration destination. This is executed under partition migration lock!
     */
    public void onPartitionMigrate(Address thisAddress, MigrationInfo migrationInfo) {
        if (!thisAddress.equals(migrationInfo.getSource())) {
            return;
        }

        int partitionId = migrationInfo.getPartitionId();
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            Iterator<ParkedOperation> it = parkQueue.iterator();
            while (it.hasNext()) {
                if (Thread.interrupted()) {
                    return;
                }
                ParkedOperation parkedOperation = it.next();
                if (!parkedOperation.isValid()) {
                    continue;
                }

                Operation op = parkedOperation.getOperation();
                if (partitionId == op.getPartitionId()) {
                    parkedOperation.setValid(false);
                    PartitionMigratingException pme = new PartitionMigratingException(thisAddress,
                            partitionId, op.getClass().getName(), op.getServiceName());
                    OperationResponseHandler responseHandler = op.getOperationResponseHandler();
                    responseHandler.sendResponse(op, pme);
                    it.remove();
                }
            }
        }
    }

    @Override
    public void cancelParkedOperations(String serviceName, Object objectId, Throwable cause) {
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            for (ParkedOperation parkedOperation : parkQueue) {
                if (!parkedOperation.isValid()) {
                    continue;
                }
                WaitNotifyKey wnk = parkedOperation.blockingOperation.getWaitKey();
                if (serviceName.equals(wnk.getServiceName())
                        && objectId.equals(wnk.getObjectName())) {
                    parkedOperation.cancel(cause);
                }
            }
        }
    }

    public void reset() {
        delayQueue.clear();
        parkQueueMap.clear();
    }

    public void shutdown() {
        logger.finest("Stopping tasks...");
        expirationTaskFuture.cancel(true);
        expirationExecutor.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        final Address thisAddress = nodeEngine.getThisAddress();
        for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
            for (ParkedOperation parkedOperation : parkQueue) {
                if (!parkedOperation.isValid()) {
                    continue;
                }

                Operation op = parkedOperation.getOperation();
                // only for local invocations, remote ones will be expired via #onMemberLeft()
                if (thisAddress.equals(op.getCallerAddress())) {
                    try {
                        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
                        responseHandler.sendResponse(op, response);
                    } catch (Exception e) {
                        logger.finest("While sending HazelcastInstanceNotActiveException response...", e);
                    }
                }
            }
            parkQueue.clear();
        }
        parkQueueMap.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OperationParker{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (Queue<ParkedOperation> scheduledOps : parkQueueMap.values()) {
            sb.append("\t");
            sb.append(scheduledOps.size());
            sb.append(", ");
        }
        sb.append("]\n}");
        return sb.toString();
    }

    private class ExpirationTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (Thread.interrupted()) {
                    return;
                }

                try {
                    if (doRun()) {
                        return;
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    logger.warning(t);
                }
            }
        }

        private boolean doRun() throws Exception {
            long waitTime = FIRST_WAIT_TIME;
            while (waitTime > 0) {
                long begin = System.currentTimeMillis();
                ParkedOperation parkedOperation = (ParkedOperation) delayQueue.poll(waitTime, MILLISECONDS);
                if (parkedOperation != null) {
                    if (parkedOperation.isValid()) {
                        invalidate(parkedOperation);
                    }
                }
                long end = System.currentTimeMillis();
                waitTime -= (end - begin);
                if (waitTime > FIRST_WAIT_TIME) {
                    waitTime = FIRST_WAIT_TIME;
                }
            }

            for (Queue<ParkedOperation> parkQueue : parkQueueMap.values()) {
                for (ParkedOperation parkedOperation : parkQueue) {
                    if (Thread.interrupted()) {
                        return true;
                    }
                    if (parkedOperation.isValid() && parkedOperation.needsInvalidation()) {
                        invalidate(parkedOperation);
                    }
                }
            }
            return false;
        }
    }
}
