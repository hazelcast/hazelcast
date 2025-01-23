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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.partition.MigrationEndpoint;
import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.ReplicaSyncEvent;
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

    private final ConcurrentMap<WaitNotifyKey, WaitSet> waitSetMap = new ConcurrentHashMap<>(100);
    private final DelayQueue delayQueue = new DelayQueue();
    private final ExecutorService expirationExecutor;
    private final Future expirationTaskFuture;
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final ConstructorFunction<WaitNotifyKey, WaitSet> waitSetConstructor
            = new ConstructorFunction<>() {
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

    // invalidated waiting ops will be removed from queue eventually by notifiers.
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
     * Invalidates all parked operations for the migrated partition if needed
     * and sends a {@link PartitionMigratingException} as a response.
     * It is also invoked for promotion.
     * This is executed under partition migration lock!
     */
    public void onPartitionMigrate(PartitionMigrationEvent migrationInfo) {

        // If this node is migration SOURCE:
        // - it will no longer be an owner (if it was before), blindly invalidate all primary operations
        //   as they will be retried on a new owner
        // - if just the backup replica index changes, backup operations should be kept to avoid backup inconsistency.
        //   In this case replica versions were already updated when backup was parked so anti-entropy would not be able
        //   to fix it. The backups would not be retired if they failed with PartitionMigratingException at this stage.
        //   Additional caveat in this case is that it is necessary to update replicaIndex in the operation so it will
        //   be allowed to execute after migration.
        //
        // If this node is migration DESTINATION:
        // - if it contained the partition before (currentReplicaIndex != -1), there can be some operations parked,
        //   either primary or backup. DESTINATION gets partition data from owner, so old operations are no longer needed.
        // - the node is treated also as DESTINATION when the backup is promoted to owner after previous owner crash.
        //   The same parked operation rules apply even though it might be tempting to keep backup operations to limit data loss.
        // - if this node did not have the partition before, there should be no parked operations for it.
        // - in case when the partition is completely lost, technically new replicas are also DESTINATION, but onPartitionMigrate
        //   is not invoked in this case. There is also nothing to do.
        //
        // The process of invalidating operations after migration should be quite strict to avoid executing
        // some stray operations when the replica is migrated back and forth (kind of ABA problem).
        //
        // Another of the safeguards against executing stray parked operations is replicaIndex checking in
        // OperationRunnerImpl.ensureNoPartitionProblems. This also the reason why it is necessary to update
        // replicaIndex for parked backup operations.
        //
        // Sometimes waiting backup operations can be cancelled after being unparked because the partition is migrating.
        // For backups this should happen only on the migration destination (both partition owner and migration destination
        // set the migrating flag, but not migration source if it is not the owner). If such failure occurs,
        // partition should be marked for replica sync as the backup operation can be treated as lost.

        if ((migrationInfo.getMigrationEndpoint() == MigrationEndpoint.SOURCE)
            || (migrationInfo.getMigrationEndpoint() == MigrationEndpoint.DESTINATION
                && migrationInfo.getCurrentReplicaIndex() >= 0)) {

            for (WaitSet waitSet : waitSetMap.values()) {
                waitSet.onPartitionMigrate(migrationInfo);
            }
        } else {
            logger.finest("onPartitionMigrate ignores %s", migrationInfo);
        }
    }

    /**
     * Invalidates all parked backup operations for the synced partition and namespace.
     * Called on operation thread.
     */
    public void onReplicaSync(ReplicaSyncEvent syncEvent) {
        // need to invalidate operation for just replicated namespace partition
        // - similar to migration DESTINATION.
        // onReplicaSync is invoked only on the destination receiving data from owner.
        for (WaitSet waitSet : waitSetMap.values()) {
            waitSet.onReplicaSync(syncEvent);
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

    // for testing
    public String dump() {
        StringBuilder sb = new StringBuilder("OperationParker{");
        sb.append("delayQueue=");
        sb.append(delayQueue.size());
        sb.append(" \n[");
        for (var entry : waitSetMap.entrySet()) {
            sb.append(entry.getKey());
            sb.append(" -> ");
            sb.append(entry.getValue());
            sb.append(",\n");
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
