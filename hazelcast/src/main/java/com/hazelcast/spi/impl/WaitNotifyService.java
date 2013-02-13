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
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.MigrationInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConcurrencyUtil.ConstructorFunction;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.logging.Level;

@PrivateApi
class WaitNotifyService {
    private final ConcurrentMap<WaitNotifyKey, Queue<WaitingOp>> mapWaitingOps = new ConcurrentHashMap<WaitNotifyKey, Queue<WaitingOp>>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final Future expirationTask;
    private final WaitingOpProcessor waitingOpProcessor;
    private final NodeEngine nodeEngine;
    private final ILogger logger;

    public WaitNotifyService(final NodeEngineImpl nodeEngine, final WaitingOpProcessor waitingOpProcessor) {
        this.nodeEngine = nodeEngine;
        this.waitingOpProcessor = waitingOpProcessor;
        final Node node = nodeEngine.getNode();
        logger = node.getLogger(WaitNotifyService.class.getName());

        expirationTask = nodeEngine.getExecutionService().submit("hz:system", new Runnable() {
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
                                    waitingOpProcessor.invalidate(waitingOp);
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
                                        waitingOpProcessor.invalidate(waitingOp);
                                    }
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    } catch (Throwable t) {
                        logger.log(Level.WARNING, t.getMessage(), t);
                    }
                }
            }
        });
    }

    private final ConstructorFunction<WaitNotifyKey, Queue<WaitingOp>> waitQueueConstructor
            = new ConstructorFunction<WaitNotifyKey, Queue<WaitingOp>>() {
        public Queue<WaitingOp> createNew(WaitNotifyKey key) {
            return new ConcurrentLinkedQueue<WaitingOp>();
        }
    };

    // runs after queue lock
    public void wait(WaitSupport so) {
        final WaitNotifyKey key = so.getWaitKey();
        final Queue<WaitingOp> q = ConcurrencyUtil.getOrPutIfAbsent(mapWaitingOps, key, waitQueueConstructor);
        long timeout = so.getWaitTimeoutMillis();
        WaitingOp waitingOp = (so instanceof KeyBasedOperation) ? new KeyBasedWaitingOp(q, so) : new WaitingOp(q, so);
        waitingOp.setNodeEngine(nodeEngine);
        if (timeout > -1 && timeout < 1500) {
            delayQueue.offer(waitingOp);
        }
        q.offer(waitingOp);
    }

    // runs after queue lock
    public void notify(Notifier notifier) {
        WaitNotifyKey key = notifier.getNotifiedKey();
        Queue<WaitingOp> q = mapWaitingOps.get(key);
        if (q == null) return;
        WaitingOp so = q.peek();
        while (so != null) {
            if (so.isValid()) {
                if (so.isExpired()) {
                    // expired
                    so.onExpire();
                } else {
                    if (so.shouldWait()) {
                        return;
                    }
                    waitingOpProcessor.processUnderExistingLock(so.getOperation());
                }
                so.setValid(false);
            }
            q.poll(); // consume
            so = q.peek();
        }
    }

    // invalidated waiting ops will removed from queue eventually by notifiers.
    public void onMemberLeft(Address leftMember) {
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
                if (Thread.interrupted()) {
                    return;
                }
                if (waitingOp.isValid()) {
                    Operation op = waitingOp.getOperation();
                    if (leftMember.equals(op.getCallerAddress())) {
                        waitingOp.setValid(false);
                    }
                }
            }
        }
    }

    // This is executed under partition migration lock!
    public void onPartitionMigrate(Address thisAddress, MigrationInfo migrationInfo) {
        if (migrationInfo.getReplicaIndex() == 0) {
            if (thisAddress.equals(migrationInfo.getFromAddress())) {
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
    }

    public void cancelWaitingOps(String serviceName, Object objectId, Throwable cause) {
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
                if (Thread.interrupted()) {
                    return;
                }
                if (waitingOp.isValid()) {
                    WaitNotifyKey wnk = waitingOp.waitSupport.getWaitKey();
                    if (serviceName.equals(wnk.getServiceName())
                            && objectId.equals(wnk.getDistributedObjectId())) {
                        waitingOp.cancel(cause);
                    }
                }
            }
        }
    }

    interface WaitingOpProcessor {

        void invalidate(WaitingOp so) throws Exception;

        void processUnderExistingLock(Operation operation);
    }

    void shutdown() {
        logger.log(Level.FINEST, "Stopping tasks...");
        expirationTask.cancel(true);
        for (Queue<WaitingOp> q : mapWaitingOps.values()) {
            for (WaitingOp waitingOp : q) {
                if (waitingOp.isValid()) {
                    final Operation op = waitingOp.getOperation();
                    // only for local invocations, remote ones will be expired via #onMemberLeft()
                    if (op.getCallId() < 0) {
                        op.getResponseHandler().sendResponse(new HazelcastInstanceNotActiveException());
                    }
                }
            }
            q.clear();
        }
        mapWaitingOps.clear();
    }

    static class KeyBasedWaitingOp extends WaitingOp implements KeyBasedOperation {
        KeyBasedWaitingOp(Queue<WaitingOp> queue, WaitSupport so) {
            super(queue, so);
        }

        public int getKeyHash() {
            return ((KeyBasedOperation) getOperation()).getKeyHash();
        }
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
            this.expirationTime = waitSupport.getWaitTimeoutMillis() < 0 ? -1
                    : Clock.currentTimeMillis() + waitSupport.getWaitTimeoutMillis();
            this.setPartitionId(op.getPartitionId());
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
            return expirationTime != -1 && Clock.currentTimeMillis() >= expirationTime;
        }

        public boolean isCancelled() {
            return error != null;
        }

        public boolean isCallTimedOut() {
            final NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            if(nodeEngine.operationService.isCallTimedOut(op)) {
                cancel(new CallTimeoutException("Call timed out for " + op.getClass().getName()));
                return true;
            }
            return false;
        }

        public boolean shouldWait() {
            return waitSupport.shouldWait();
        }

        public long getDelay(TimeUnit unit) {
            return unit.convert(expirationTime - Clock.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

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
                if (isCancelled() && queue.remove(this)) {
                    op.getResponseHandler().sendResponse(error);
                } else if (isExpired() && queue.remove(this)) {
                    waitSupport.onWaitExpire();
                }
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
            sb.append("[").append(hashCode()).append("] ");
            sb.append("{op=").append(op);
            sb.append(", expirationTime=").append(expirationTime);
            sb.append(", valid=").append(valid);
            sb.append('}');
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SchedulingService{");
        sb.append("delayQueue=" + delayQueue.size());
        sb.append(" \n[");
        for (Queue<WaitingOp> ScheduledOps : mapWaitingOps.values()) {
            sb.append("\t");
            sb.append(ScheduledOps.size() + ", ");
        }
        sb.append("]\n}");
        return sb.toString();
    }
}
