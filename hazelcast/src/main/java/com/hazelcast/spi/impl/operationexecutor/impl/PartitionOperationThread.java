/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_PARTITION_OPERATION_THREAD_NORMAL_PENDING_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_PARTITION_OPERATION_THREAD_PRIORITY_PENDING_COUNT;

/**
 * An {@link OperationThread} that executes Operations for a particular partition,
 * e.g. a map.get operation.
 */
public final class PartitionOperationThread extends OperationThread {

    private final OperationRunner[] partitionOperationRunners;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public PartitionOperationThread(String name,
                                    int threadId,
                                    OperationQueue queue,
                                    ILogger logger,
                                    NodeExtension nodeExtension,
                                    OperationRunner[] partitionOperationRunners,
                                    ClassLoader configClassLoader) {
        super(name, threadId, queue, logger, nodeExtension, false, configClassLoader);
        this.partitionOperationRunners = partitionOperationRunners;
    }

    public int activePartitionThreads;
    public PartitionOperationThread[] partitionOperationThreads;

    /**
     * For each partition there is a {@link OperationRunner} instance. So we need
     * to find the right one based on the partition ID.
     */
    @Override
    public OperationRunner operationRunner(int partitionId) {
        return partitionOperationRunners[partitionId];
    }

    @Probe(name = OPERATION_METRIC_PARTITION_OPERATION_THREAD_PRIORITY_PENDING_COUNT)
    int priorityPendingCount() {
        return queue.prioritySize();
    }

    @Probe(name = OPERATION_METRIC_PARTITION_OPERATION_THREAD_NORMAL_PENDING_COUNT)
    int normalPendingCount() {
        return queue.normalSize();
    }

    @Override
    protected void process(Operation operation) {
        int partitionId = operation.getPartitionId();
        if (partitionId >= 0) {
            int expectedThreadId = partitionId % activePartitionThreads;
            if (expectedThreadId != threadId) {
                partitionOperationThreads[expectedThreadId].queue.add(operation, operation.isUrgent());
                return;
            }
        }

        currentRunner = operationRunner(partitionId);
        currentRunner.run(operation);
        completedOperationCount.inc();
    }

    @Override
    protected void process(Packet packet) throws Exception {
        int partitionId = packet.getPartitionId();
        if (partitionId >= 0) {
            int expectedThreadId = partitionId % activePartitionThreads;
            if (expectedThreadId != threadId) {
                partitionOperationThreads[expectedThreadId].queue.add(packet, packet.isUrgent());
                return;
            }
        }

        currentRunner = operationRunner(partitionId);
        currentRunner.run(packet);
        completedPacketCount.inc();
    }

    @Override
    protected void process(PartitionSpecificRunnable runnable) {
        int partitionId = runnable.getPartitionId();
        if (partitionId >= 0) {
            int expectedThreadId = partitionId % activePartitionThreads;
            if (expectedThreadId != threadId) {
                partitionOperationThreads[expectedThreadId].queue.add(runnable, runnable instanceof UrgentSystemOperation);
                return;
            }
        }

        currentRunner = operationRunner(runnable.getPartitionId());
        currentRunner.run(runnable);
        completedPartitionSpecificRunnableCount.inc();
    }
}
