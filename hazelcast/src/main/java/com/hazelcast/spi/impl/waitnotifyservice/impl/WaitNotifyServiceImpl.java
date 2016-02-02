/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.waitnotifyservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitNotifyService;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.OperationTracingService;
import com.hazelcast.spi.impl.waitnotifyservice.InternalWaitNotifyService;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class WaitNotifyServiceImpl implements InternalWaitNotifyService, OperationTracingService {

    private static final long FIRST_WAIT_TIME = 1000;
    private static final long TIMEOUT_UPPER_BOUND = 1500;

    private final ConcurrentMap<WaitNotifyKey, Queue<WaitingOperation>> blockedOperationsMap =
            new ConcurrentHashMap<WaitNotifyKey, Queue<WaitingOperation>>(100);

    // todo: contention point.
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationService;
    private final Future expirationTask;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private final ConstructorFunction<WaitNotifyKey, Queue<WaitingOperation>> waitQueueConstructor
            = new ConstructorFunction<WaitNotifyKey, Queue<WaitingOperation>>() {
        @Override
        public Queue<WaitingOperation> createNew(WaitNotifyKey key) {
            return new ConcurrentLinkedQueue<WaitingOperation>();
        }
    };

    public WaitNotifyServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        Node node = nodeEngine.getNode();
        logger = node.getLogger(WaitNotifyService.class.getName());

        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
        expirationService = newSingleThreadExecutor(
                new SingleExecutorThreadFactory(threadGroup.getInternalThreadGroup(),
                        threadGroup.getClassLoader(),
                        threadGroup.getThreadNamePrefix("wait-notify")));

        expirationTask = expirationService.submit(new ExpirationTask());
    }

    @Override
    public void scan(Map<Address, List<Long>> result) {
        for (Queue<WaitingOperation> queue : blockedOperationsMap.values()) {

            for (WaitingOperation op : queue) {
                Address callerAddress = op.getCallerAddress();
                if (callerAddress == null) {
                    // this check sucks; we should rely on getCallerAddress to be valid.
                    callerAddress = nodeEngine.getThisAddress();
                }

                List<Long> callIds = result.get(callerAddress);
                if (callIds == null) {
                    callIds = new ArrayList<Long>();
                    result.put(callerAddress, callIds);
                }

                callIds.add(op.getCallId());
            }
        }
    }

    @Override
    public void interrupt(WaitNotifyKey key, String callerUUID, long callId) {
        WaitingOperation waitingOp = deleteOperation(key, callerUUID, callId);
        if (waitingOp == null) {
            return;
        }

        // we mark the WaitingOperation as interrupted
        waitingOp.setInterrupted();
        // and then we schedule the Waiting op. Is will take the right choice
        nodeEngine.getOperationService().executeOperation(waitingOp);
    }

    private WaitingOperation deleteOperation(WaitNotifyKey key, String callerUUID, long callId) {
        Queue<WaitingOperation> operations = blockedOperationsMap.get(key);
        if (operations == null) {
            return null;
        }

        Iterator<WaitingOperation> it = operations.iterator();
        while (it.hasNext()) {
            WaitingOperation waitingOperation = it.next();
            Operation targetOperation = waitingOperation.getOperation();
            if (targetOperation.getCallId() == callId && targetOperation.getCallerUuid().equals(callerUUID)) {
                it.remove();
                return waitingOperation;
            }
        }

        return null;
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void await(WaitSupport waitSupport) {
        WaitNotifyKey key = waitSupport.getWaitKey();
        Queue<WaitingOperation> q = getOrPutIfAbsent(blockedOperationsMap, key, waitQueueConstructor);
        long timeout = waitSupport.getWaitTimeout();
        WaitingOperation waitingOp = new WaitingOperation(q, waitSupport);
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
    public void notify(Notifier notifier) {
        WaitNotifyKey key = notifier.getNotifiedKey();
        Queue<WaitingOperation> q = blockedOperationsMap.get(key);
        if (q == null) {
            return;
        }
        WaitingOperation waitingOp = q.peek();
        while (waitingOp != null) {
            Operation op = waitingOp.getOperation();
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
                    nodeEngine.getOperationService().runOperationOnCallingThread(op);
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
                blockedOperationsMap.remove(key);
            }
        }
    }

    // for testing purposes only
    public int getAwaitQueueCount() {
        return blockedOperationsMap.size();
    }

    // for testing purposes only
    public int getTotalWaitingOperationCount() {
        int count = 0;
        for (Queue<WaitingOperation> queue : blockedOperationsMap.values()) {
            count += queue.size();
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
        for (Queue<WaitingOperation> q : blockedOperationsMap.values()) {
            for (WaitingOperation waitingOp : q) {
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
        for (Queue<WaitingOperation> q : blockedOperationsMap.values()) {
            Iterator<WaitingOperation> it = q.iterator();
            while (it.hasNext()) {
                if (Thread.interrupted()) {
                    return;
                }
                WaitingOperation waitingOp = it.next();
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
    public void cancelWaitingOps(String serviceName, Object objectId, Throwable cause) {
        for (Queue<WaitingOperation> q : blockedOperationsMap.values()) {
            for (WaitingOperation waitingOp : q) {
                if (waitingOp.isValid()) {
                    WaitNotifyKey wnk = waitingOp.waitSupport.getWaitKey();
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
        blockedOperationsMap.clear();
    }

    public void shutdown() {
        logger.finest("Stopping tasks...");
        expirationTask.cancel(true);
        expirationService.shutdown();
        Object response = new HazelcastInstanceNotActiveException();
        Address thisAddress = nodeEngine.getThisAddress();
        for (Queue<WaitingOperation> q : blockedOperationsMap.values()) {
            for (WaitingOperation waitingOp : q) {
                if (waitingOp.isValid()) {
                    Operation op = waitingOp.getOperation();
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
        blockedOperationsMap.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WaitNotifyService{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (Queue<WaitingOperation> scheduledOps : blockedOperationsMap.values()) {
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
                long begin = currentTimeMillis();
                WaitingOperation waitingOp = (WaitingOperation) delayQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                if (waitingOp != null) {
                    if (waitingOp.isValid()) {
                        invalidate(waitingOp);
                    }
                }
                long end = currentTimeMillis();
                waitTime -= (end - begin);
                if (waitTime > FIRST_WAIT_TIME) {
                    waitTime = FIRST_WAIT_TIME;
                }
            }

            for (Queue<WaitingOperation> q : blockedOperationsMap.values()) {
                for (WaitingOperation waitingOp : q) {
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

        private void invalidate(WaitingOperation waitingOp) throws Exception {
            nodeEngine.getOperationService().executeOperation(waitingOp);
        }
    }
}
