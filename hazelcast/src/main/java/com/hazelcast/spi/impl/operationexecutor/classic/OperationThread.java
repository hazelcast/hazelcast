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

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.util.counters.SwCounter;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.util.counters.SwCounter.newSwCounter;

/**
 * The OperationThread is responsible for processing operations, packets containing operations and runnable's.
 * <p/>
 * There are 2 flavors of OperationThread:
 * - threads that deal with operations for a specific partition
 * - threads that deal with non partition specific tasks
 * <p/>
 * The actual processing of an operation is forwarded to the {@link com.hazelcast.spi.impl.operationexecutor.OperationRunner}.
 */
public abstract class OperationThread extends HazelcastManagedThread {

    final int threadId;
    final ScheduleQueue scheduleQueue;

    // All these counters are updated by this OperationThread (so a single writer) and are read by the MetricsRegistry.
    @Probe
    private final SwCounter processedTotalCount = newSwCounter();
    @Probe
    private final SwCounter processedPacketCount = newSwCounter();
    @Probe
    private final SwCounter processedOperationsCount = newSwCounter();
    @Probe
    private final SwCounter processedPartitionSpecificRunnableCount = newSwCounter();

    private final NodeExtension nodeExtension;
    private final ILogger logger;
    private volatile boolean shutdown;

    // This field wil only be accessed by the thread itself when doing 'self' calls. So no need
    // for any form of synchronization.
    private OperationRunner currentOperationRunner;

    public OperationThread(String name, int threadId, ScheduleQueue scheduleQueue,
                           ILogger logger, HazelcastThreadGroup threadGroup, NodeExtension nodeExtension) {
        super(threadGroup.getInternalThreadGroup(), name);
        setContextClassLoader(threadGroup.getClassLoader());
        this.scheduleQueue = scheduleQueue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
    }

    @Probe
    int priorityPendingCount() {
        return scheduleQueue.normalSize();
    }

    @Probe
    int normalPendingCount() {
        return scheduleQueue.normalSize();
    }

    public OperationRunner getCurrentOperationRunner() {
        return currentOperationRunner;
    }

    public abstract OperationRunner getOperationRunner(int partitionId);

    @Override
    public final void run() {
        nodeExtension.onThreadStart(this);
        try {
            doRun();
        } catch (Throwable t) {
            inspectOutputMemoryError(t);
            logger.severe(t);
        } finally {
            nodeExtension.onThreadStop(this);
        }
    }

    private void doRun() {
        for (; ; ) {
            Object task;
            try {
                task = scheduleQueue.take();
            } catch (InterruptedException e) {
                if (shutdown) {
                    return;
                }
                continue;
            }

            if (shutdown) {
                return;
            }

            process(task);
        }
    }

    private void process(Object task) {
        processedTotalCount.inc();

        if (task instanceof Operation) {
            processOperation((Operation) task);
            return;
        }

        if (task instanceof Packet) {
            processPacket((Packet) task);
            return;
        }

        if (task instanceof PartitionSpecificRunnable) {
            processPartitionSpecificRunnable((PartitionSpecificRunnable) task);
            return;
        }

        throw new IllegalStateException("Unhandled task type for task:" + task);
    }

    private void processPartitionSpecificRunnable(PartitionSpecificRunnable runnable) {
        processedPartitionSpecificRunnableCount.inc();

        currentOperationRunner = getOperationRunner(runnable.getPartitionId());
        try {
            currentOperationRunner.run(runnable);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process task: " + runnable + " on " + getName(), e);
        } finally {
            currentOperationRunner = null;
        }
    }

    private void processPacket(Packet packet) {
        processedPacketCount.inc();

        currentOperationRunner = getOperationRunner(packet.getPartitionId());
        try {
            currentOperationRunner.run(packet);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process packet: " + packet + " on " + getName(), e);
        } finally {
            currentOperationRunner = null;
        }
    }

    private void processOperation(Operation operation) {
        processedOperationsCount.inc();

        currentOperationRunner = getOperationRunner(operation.getPartitionId());
        try {
            currentOperationRunner.run(operation);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process operation: " + operation + " on " + getName(), e);
        } finally {
            currentOperationRunner = null;
        }
    }

    public final void shutdown() {
        shutdown = true;
        interrupt();
    }

    public final void awaitTermination(int timeout, TimeUnit unit) throws InterruptedException {
        long timeoutMs = unit.toMillis(timeout);
        join(timeoutMs);
    }
}
