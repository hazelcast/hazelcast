/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
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
import com.hazelcast.util.ConcurrencyUtil;
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
import java.util.concurrent.TimeUnit;

public class OperationParkerImpl implements OperationParker, LiveOperationsTracker {

    private static final long FIRST_WAIT_TIME = 1000;
    private static final long TIMEOUT_UPPER_BOUND = 1500;

    private final ConcurrentMap<WaitNotifyKey, Queue<ParkedOperation>> mapWaitingOps =
            new ConcurrentHashMap<WaitNotifyKey, Queue<ParkedOperation>>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationService;
    private final Future expirationTask;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final ConstructorFunction<WaitNotifyKey, Queue<ParkedOperation>> waitQueueConstructor
            = new ConstructorFunction<WaitNotifyKey, Queue<ParkedOperation>>() {
        @Override
        public Queue<ParkedOperation> createNew(WaitNotifyKey key) {
            return new ConcurrentLinkedQueue<ParkedOperation>();
        }
    };

    public OperationParkerImpl(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(OperationParker.class.getName());

        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
        expirationService = Executors.newSingleThreadExecutor(
                new SingleExecutorThreadFactory(threadGroup.getInternalThreadGroup(),
                        threadGroup.getClassLoader(),
                        threadGroup.getThreadNamePrefix("wait-notify")));

        expirationTask = expirationService.submit(new ExpirationTask());
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (Queue<ParkedOperation> queue : mapWaitingOps.values()) {
            for (ParkedOperation op : queue) {
                liveOperations.add(op.getCallerAddress(), op.getCallId());
            }
        }
    }

    private void invalidate(final ParkedOperation waitingOp) throws Exception {
        nodeEngine.getOperationService().execute(waitingOp);
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void park(BlockingOperation op) {
        final WaitNotifyKey key = op.getWaitKey();
        final Queue<ParkedOperation> q = ConcurrencyUtil.getOrPutIfAbsent(mapWaitingOps, key, waitQueueConstructor);
        long timeout = op.getWaitTimeout();
        ParkedOperation waitingOp = new ParkedOperation(q, op);
        waitingOp.setNodeEngine(nodeEngine);
        q.offer(waitingOp);
        if (timeout > -1 && timeout < TIMEOUT_UPPER_BOUND) {
            delayQueue.offer(waitingOp);
        }
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void unpark(Notifier notifier) {
        WaitNotifyKey key = notifier.getNotifiedKey();
        Queue<ParkedOperation> q = mapWaitingOps.get(key);
        if (q == null) {
            return;
        }
        ParkedOperation waitingOp = q.peek();
        while (waitingOp != null) {
            final Operation op = waitingOp.getOperation();
            if (notifier == op) {
                throw new IllegalStateException("Found cyclic wait-notify! -> " + notifier);
            }
            if (waitingOp.isValid()) {
                if (waitingOp.isExpired()) {
                    // expired
                    waitingOp.onExpire();
                } else {
                    if (waitingOp.shouldWait()) {
                        return;
                    }
                    nodeEngine.getOperationService().run(op);
                }
                waitingOp.setValid(false);
            }
            // consume
            q.poll();

            waitingOp = q.peek();

            // If q.peek() returns null, we should deregister this specific
            // key to avoid memory leak. By contract we know that await() and notify()
            // cannot be called in parallel.
            // We can safely remove this queue from registration map here.
            if (waitingOp == null) {
                mapWaitingOps.remove(key);
            }
        }
    }

    // for testing purposes only
    public int getAwaitQueueCount() {
        return mapWaitingOps.size();
    }

    // for testing purposes only
    public int getTotalWaitingOperationCount() {
        int count = 0;
        for (Queue<ParkedOperation> queue : mapWaitingOps.values()) {
            count += queue.size();
        }
        return count;
    }

    // for testing purposes only
    public int getTotalValidWaitingOperationCount() {
        int count = 0;
        for (Queue<ParkedOperation> queue : mapWaitingOps.values()) {
            for (ParkedOperation parkedOperation : queue) {
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
        for (Queue<ParkedOperation> q : mapWaitingOps.values()) {
            for (ParkedOperation waitingOp : q) {
                if (waitingOp.isValid()) {
                    Operation op = waitingOp.getOperation();
                    if (callerUuid.equals(op.getCallerUuid())) {
                        waitingOp.setValid(false);
                    }
                }
            }
        }
    }

    // This is executed under partition migration lock!
    public void onPartitionMigrate(Address thisAddress, MigrationInfo migrationInfo) {
        if (!thisAddress.equals(migrationInfo.getSource())) {
            return;
        }

        int partitionId = migrationInfo.getPartitionId();
        for (Queue<ParkedOperation> q : mapWaitingOps.values()) {
            Iterator<ParkedOperation> it = q.iterator();
            while (it.hasNext()) {
                if (Thread.interrupted()) {
                    return;
                }
                ParkedOperation waitingOp = it.next();
                if (waitingOp.isValid()) {
                    Operation op = waitingOp.getOperation();
                    if (partitionId == op.getPartitionId()) {
                        waitingOp.setValid(false);
                        PartitionMigratingException pme = new PartitionMigratingException(thisAddress,
                                partitionId, op.getClass().getName(), op.getServiceName());
                        OperationResponseHandler responseHandler = op.getOperationResponseHandler();
                        responseHandler.sendResponse(op, pme);
                        it.remove();
                    }
                }
            }
        }
    }

    @Override
    public void cancelParkedOperations(String serviceName, Object objectId, Throwable cause) {
        for (Queue<ParkedOperation> q : mapWaitingOps.values()) {
            for (ParkedOperation waitingOp : q) {
                if (waitingOp.isValid()) {
                    WaitNotifyKey wnk = waitingOp.blockingOperation.getWaitKey();
                    if (serviceName.equals(wnk.getServiceName())
                            && objectId.equals(wnk.getObjectName())) {
                        waitingOp.cancel(cause);
                    }
                }
            }
        }
    }

    public void reset() {
        delayQueue.clear();
        mapWaitingOps.clear();
    }

    public void shutdown() {
        logger.finest("Stopping tasks...");
        expirationTask.cancel(true);
        expirationService.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        final Address thisAddress = nodeEngine.getThisAddress();
        for (Queue<ParkedOperation> q : mapWaitingOps.values()) {
            for (ParkedOperation waitingOp : q) {
                if (waitingOp.isValid()) {
                    final Operation op = waitingOp.getOperation();
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
            }
            q.clear();
        }
        mapWaitingOps.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OperationParker{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (Queue<ParkedOperation> scheduledOps : mapWaitingOps.values()) {
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
                ParkedOperation waitingOp = (ParkedOperation) delayQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                if (waitingOp != null) {
                    if (waitingOp.isValid()) {
                        invalidate(waitingOp);
                    }
                }
                long end = System.currentTimeMillis();
                waitTime -= (end - begin);
                if (waitTime > FIRST_WAIT_TIME) {
                    waitTime = FIRST_WAIT_TIME;
                }
            }

            for (Queue<ParkedOperation> q : mapWaitingOps.values()) {
                for (ParkedOperation waitingOp : q) {
                    if (Thread.interrupted()) {
                        return true;
                    }
                    if (waitingOp.isValid() && waitingOp.needsInvalidation()) {
                        invalidate(waitingOp);
                    }
                }
            }
            return false;
        }
    }
}
