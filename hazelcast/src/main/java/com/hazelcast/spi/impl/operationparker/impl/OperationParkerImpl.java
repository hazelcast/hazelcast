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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.partition.MigrationInfo;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.executor.SingleExecutorThreadFactory;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.TargetDisconnectedException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationparker.OperationParker;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_PARKER_PARK_QUEUE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_PARKER_TOTAL_PARKED_OPERATION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_PARKER;
import static com.hazelcast.internal.util.ConcurrencyUtil.getOrPutIfAbsent;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OperationParkerImpl implements OperationParker, LiveOperationsTracker, StaticMetricsProvider {

    private static final long FIRST_WAIT_TIME = 1000;

    private final ConcurrentMap<WaitNotifyKey, WaitSet> waitSetMap = new ConcurrentHashMap<WaitNotifyKey, WaitSet>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationExecutor;
    private final Future expirationTaskFuture;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ConstructorFunction<WaitNotifyKey, WaitSet> waitSetConstructor
            = new ConstructorFunction<WaitNotifyKey, WaitSet>() {
        @Override
        public WaitSet createNew(WaitNotifyKey key) {
            return new WaitSet(logger, nodeEngine, waitSetMap, delayQueue);
        }
    };

    public OperationParkerImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        Node node = nodeEngine.getNode();
        this.logger = node.getLogger(OperationParker.class);
        this.expirationExecutor = Executors.newSingleThreadExecutor(
                new SingleExecutorThreadFactory(node.getConfigClassLoader(),
                        createThreadName(nodeEngine.getHazelcastInstance().getName(), "operation-parker")));
        this.expirationTaskFuture = expirationExecutor.submit(new ExpirationTask());
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        registry.registerStaticMetrics(this, OPERATION_PREFIX_PARKER);
    }

    @Override
    public void populate(LiveOperations liveOperations) {
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.populate(liveOperations);
        }
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void park(BlockingOperation op) {
        WaitSet waitSet = getOrPutIfAbsent(waitSetMap, op.getWaitKey(), waitSetConstructor);
        waitSet.park(op);
    }

    // Runs in operation thread, we can assume that
    // here we have an implicit lock for specific WaitNotifyKey.
    // see javadoc
    @Override
    public void unpark(Notifier notifier) {
        WaitNotifyKey waitNotifyKey = notifier.getNotifiedKey();
        WaitSet waitSet = waitSetMap.get(waitNotifyKey);
        if (waitSet != null) {
            waitSet.unpark(notifier, waitNotifyKey);
        }
    }

    @Probe(name = OPERATION_METRIC_PARKER_PARK_QUEUE_COUNT)
    public int getParkQueueCount() {
        return waitSetMap.size();
    }

    @Probe(name = OPERATION_METRIC_PARKER_TOTAL_PARKED_OPERATION_COUNT)
    public int getTotalParkedOperationCount() {
        int count = 0;
        for (WaitSet waitSet : waitSetMap.values()) {
            count += waitSet.size();
        }
        return count;
    }

    // for testing purposes only
    public int getTotalValidWaitingOperationCount() {
        int count = 0;
        for (WaitSet waitSet : waitSetMap.values()) {
            count += waitSet.totalValidWaitingOperationCount();
        }
        return count;
    }

    // invalidated waiting ops will removed from queue eventually by notifiers.
    public void onMemberLeft(MemberImpl leftMember) {
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.invalidateAll(leftMember.getUuid());
        }
    }

    public void onClientDisconnected(UUID clientUuid) {
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.cancelAll(clientUuid, new TargetDisconnectedException("Client disconnected: " + clientUuid));
        }
    }

    /**
     * Invalidates all parked operations for the migrated partition and sends a {@link PartitionMigratingException} as a
     * response.
     * Invoked on the migration destination. This is executed under partition migration lock!
     */
    public void onPartitionMigrate(MigrationInfo migrationInfo) {
        if (migrationInfo.getSource() == null || !migrationInfo.getSource().isIdentical(nodeEngine.getLocalMember())) {
            return;
        }

        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.onPartitionMigrate(migrationInfo);
        }
    }

    @Override
    public void cancelParkedOperations(String serviceName, Object objectId, Throwable cause) {
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.cancelAll(serviceName, objectId, cause);
        }
    }

    public void reset() {
        delayQueue.clear();
        waitSetMap.clear();
    }

    public void shutdown() {
        logger.finest("Stopping tasks...");
        expirationTaskFuture.cancel(true);
        expirationExecutor.shutdownNow();
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.onShutdown();
        }
        waitSetMap.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("OperationParker{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (WaitSet waitSet : waitSetMap.values()) {
            sb.append("\t");
            sb.append(waitSet.size());
            sb.append(", ");
        }
        sb.append("]\n}");
        return sb.toString();
    }

    private class ExpirationTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (Thread.currentThread().isInterrupted()) {
                    return;
                }

                try {
                    if (doRun()) {
                        return;
                    }
                } catch (InterruptedException e) {
                    // restore the interrupt
                    Thread.currentThread().interrupt();
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
                WaitSetEntry entry = (WaitSetEntry) delayQueue.poll(waitTime, MILLISECONDS);
                if (entry != null) {
                    if (entry.isValid()) {
                        invalidate(entry);
                    }
                }
                long end = System.currentTimeMillis();
                waitTime -= (end - begin);
                if (waitTime > FIRST_WAIT_TIME) {
                    waitTime = FIRST_WAIT_TIME;
                }
            }

            for (WaitSet waitSet : waitSetMap.values()) {
                if (Thread.interrupted()) {
                    return true;
                }

                for (WaitSetEntry entry : waitSet) {
                    if (entry.isValid() && entry.needsInvalidation()) {
                        invalidate(entry);
                    }
                }
            }
            return false;
        }

        private void invalidate(WaitSetEntry entry) {
            nodeEngine.getOperationService().execute(entry);
        }
    }
}
