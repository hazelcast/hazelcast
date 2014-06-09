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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.PartitionAwareOperation;
import com.hazelcast.spi.WaitNotifyKey;
import com.hazelcast.spi.WaitNotifyService;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.executor.SingleExecutorThreadFactory;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

class WaitNotifyServiceImpl implements WaitNotifyService {

    private final ConcurrentMap<WaitNotifyKey, Queue<WaitingOp>> mapWaitingOps = new ConcurrentHashMap<WaitNotifyKey, Queue<WaitingOp>>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationService;
    private final Future expirationTask;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    public WaitNotifyServiceImpl(final NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(WaitNotifyService.class.getName());

        String threadNamePrefix = node.getThreadNamePrefix("wait-notify");
        expirationService = Executors.newSingleThreadExecutor(
                new SingleExecutorThreadFactory(node.threadGroup, node.getConfigClassLoader(), threadNamePrefix));

        expirationTask = expirationService.submit(new ExpirationTask());
    }

    private void invalidate(final WaitingOp waitingOp) throws Exception {
        nodeEngine.getOperationService().executeOperation(waitingOp);
    }

    private final ConstructorFunction<WaitNotifyKey, Queue<WaitingOp>> waitQueueConstructor
            = new ConstructorFunction<WaitNotifyKey, Queue<WaitingOp>>() {
        public Queue<WaitingOp> createNew(WaitNotifyKey key) {
            return new ConcurrentLinkedQueue<WaitingOp>();
        }
    };

    // runs after queue lock
    @Override
    public void await(WaitSupport waitSupport) {
        final WaitNotifyKey key = waitSupport.getWaitKey();
        final Queue<WaitingOp> q = ConcurrencyUtil.getOrPutIfAbsent(mapWaitingOps, key, waitQueueConstructor);
        long timeout = waitSupport.getWaitTimeout();
        WaitingOp waitingOp = new WaitingOp(q, waitSupport);
        waitingOp.setNodeEngine(nodeEngine);
        q.offer(waitingOp);
        if (timeout > -1 && timeout < 1500) {
            delayQueue.offer(waitingOp);
        }
    }

    // runs after queue lock
    @Override
    public void notify(Notifier notifier) {
        WaitNotifyKey key = notifier.getNotifiedKey();
        Queue<WaitingOp> q = mapWaitingOps.get(key);
        if (q == null) return;
        WaitingOp waitingOp = q.peek();
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
                    nodeEngine.operationService.runOperationOnCallingThread(op);
                }
                waitingOp.setValid(false);
            }
            q.poll(); // consume
            waitingOp = q.peek();
        }
    }

    // invalidated waiting ops will removed from queue eventually by notifiers.
    void onMemberLeft(MemberImpl leftMember) {
        invalidateWaitingOps(leftMember.getUuid());
    }

    void onClientDisconnected(String clientUuid) {
        invalidateWaitingOps(clientUuid);
    }

    private void invalidateWaitingOps(String callerUuid) {
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
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
    void onPartitionMigrate(Address thisAddress, MigrationInfo migrationInfo) {
        if (thisAddress.equals(migrationInfo.getSource())) {
            int partitionId = migrationInfo.getPartitionId();
            for (Queue<WaitingOp> q : mapWaitingOps.values()) {
                Iterator<WaitingOp> it = q.iterator();
                while (it.hasNext()) {
                    if (Thread.interrupted()) {
                        return;
                    }
                    WaitingOp waitingOp = it.next();
                    if (waitingOp.isValid()) {
                        Operation op = waitingOp.getOperation();
                        if (partitionId == op.getPartitionId()) {
                            waitingOp.setValid(false);
                            PartitionMigratingException pme = new PartitionMigratingException(thisAddress,
                                    partitionId, op.getClass().getName(), op.getServiceName());
                            op.getResponseHandler().sendResponse(pme);
                            it.remove();
                        }
                    }
                }
            }
        }
    }

    public void cancelWaitingOps(String serviceName, Object objectId, Throwable cause) {
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
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

    void shutdown() {
        logger.finest("Stopping tasks...");
        expirationTask.cancel(true);
        expirationService.shutdown();
        final Object response = new HazelcastInstanceNotActiveException();
        final Address thisAddress = nodeEngine.getThisAddress();
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
                if (waitingOp.isValid()) {
                    final Operation op = waitingOp.getOperation();
                    // only for local invocations, remote ones will be expired via #onMemberLeft()
                    if (thisAddress.equals(op.getCallerAddress())) {
                        try {
                            op.getResponseHandler().sendResponse(response);
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

    static class WaitingOp extends AbstractOperation implements Delayed, PartitionAwareOperation {
        final Queue<WaitingOp> queue;
        final Operation op;
        final WaitSupport waitSupport;
        final long expirationTime;
        volatile boolean valid = true;
        volatile Throwable error = null;

        WaitingOp(Queue<WaitingOp> queue, WaitSupport waitSupport) {
            this.op = (Operation) waitSupport;
            this.waitSupport = waitSupport;
            this.queue = queue;
            this.expirationTime = getExpirationTime(waitSupport);
            this.setPartitionId(op.getPartitionId());
        }

        private long getExpirationTime(WaitSupport waitSupport) {
            long waitTimeout = waitSupport.getWaitTimeout();
            if (waitTimeout < 0) {
                return -1;
            }
            long expirationTime = Clock.currentTimeMillis() + waitTimeout;
            if (expirationTime < 0) {
                return -1;
            }
            return expirationTime;
        }

        public Operation getOperation() {
            return op;
        }

        public void setValid(boolean valid) {
            this.valid = valid;
        }

        public boolean isValid() {
            return valid;
        }

        public boolean needsInvalidation() {
            return isExpired() || isCancelled() || isCallTimedOut();
        }

        public boolean isExpired() {
            return expirationTime > 0 && Clock.currentTimeMillis() >= expirationTime;
        }

        public boolean isCancelled() {
            return error != null;
        }

        public boolean isCallTimedOut() {
            final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            if (nodeEngine.operationService.isCallTimedOut(op)) {
                cancel(new CallTimeoutException(op.getClass().getName(), op.getInvocationTime(), op.getCallTimeout()));
                return true;
            }
            return false;
        }

        public boolean shouldWait() {
            return waitSupport.shouldWait();
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(expirationTime - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            if (other == this) // compare zero ONLY if same object
                return 0;
            long d = (getDelay(TimeUnit.NANOSECONDS) -
                    other.getDelay(TimeUnit.NANOSECONDS));
            return (d == 0) ? 0 : ((d < 0) ? -1 : 1);
        }

        @Override
        public void run() throws Exception {
            if (valid) {
                boolean expired = isExpired();
                boolean cancelled = isCancelled();
                if (expired || cancelled) {
                    if (queue.remove(this)) {
                        valid = false;
                        if (expired) {
                            waitSupport.onWaitExpire();
                        } else {
                            op.getResponseHandler().sendResponse(error);
                        }
                    }
                }
            }
        }

        //If you don't think instances of this class will ever be inserted into a HashMap/HashTable,
        // the recommended hashCode implementation to use is:
        @Override
        public int hashCode() {
            assert false : "hashCode not designed";
            return 42; // any arbitrary constant will do
        }

        @Override
        // use object.equals
        public boolean equals(Object obj) {
            return super.equals(obj);
        }

        @Override
        public void logError(Throwable e) {
            final ILogger logger = getLogger();
            if (e instanceof RetryableException) {
                logger.warning("Op: " + op + ", " + e.getClass().getName() + ": " + e.getMessage());
            } else if (e instanceof OutOfMemoryError) {
                try {
                    logger.log(Level.SEVERE, e.getMessage(), e);
                } catch (Throwable ignored) {
                }
            } else {
                logger.severe("Op: " + op + ", Error: " + e.getMessage(), e);
            }
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        public String getServiceName() {
            return op.getServiceName();
        }

        public void onExpire() {
            waitSupport.onWaitExpire();
        }

        public void cancel(Throwable t) {
            error = t;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();
            sb.append("WaitingOp");
            sb.append("{op=").append(op);
            sb.append(", expirationTime=").append(expirationTime);
            sb.append(", valid=").append(valid);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("WaitNotifyService{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (Queue<WaitingOp> ScheduledOps : mapWaitingOps.values()) {
            sb.append("\t");
            sb.append(ScheduledOps.size());
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
                    long waitTime = 1000;
                    while (waitTime > 0) {
                        long begin = System.currentTimeMillis();
                        WaitingOp waitingOp = (WaitingOp) delayQueue.poll(waitTime, TimeUnit.MILLISECONDS);
                        if (waitingOp != null) {
                            if (waitingOp.isValid()) {
                                invalidate(waitingOp);
                            }
                        }
                        long end = System.currentTimeMillis();
                        waitTime -= (end - begin);
                        if (waitTime > 1000) {
                            waitTime = 1000;
                        }
                    }
                    for (Queue<WaitingOp> q : mapWaitingOps.values()) {
                        for (WaitingOp waitingOp : q) {
                            if (Thread.interrupted()) {
                                return;
                            }
                            if (waitingOp.isValid()) {
                                if (waitingOp.needsInvalidation()) {
                                    invalidate(waitingOp);
                                }
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    return;
                } catch (Throwable t) {
                    logger.warning(t);
                }
            }
        }
    }
}
