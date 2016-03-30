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

package com.hazelcast.spi.impl.operationexecutor.classic;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;

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
    final OperationQueue queue;

    // All these counters are updated by this OperationThread (so a single writer) and are read by the MetricsRegistry.
    @Probe
    private final SwCounter processedTotal = newSwCounter();
    @Probe
    private final SwCounter processedPackets = newSwCounter();
    @Probe
    private final SwCounter processedOperations = newSwCounter();
    @Probe
    private final SwCounter processedPartitionSpecificRunnables = newSwCounter();
    @Probe
    private final SwCounter processedRunnables = newSwCounter();

    private final NodeExtension nodeExtension;
    private final ILogger logger;
    private volatile boolean shutdown;

    // This field wil only be accessed by the thread itself when doing 'self' calls. So no need
    // for any form of synchronization.
    private OperationRunner currentRunner;

    public OperationThread(String name, int threadId, OperationQueue queue,
                           ILogger logger, HazelcastThreadGroup threadGroup, NodeExtension nodeExtension) {
        super(threadGroup.getInternalThreadGroup(), name);
        setContextClassLoader(threadGroup.getClassLoader());
        this.queue = queue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
    }

    @Probe
    int priorityPendingCount() {
        return queue.prioritySize();
    }

    @Probe
    int normalPendingCount() {
        return queue.normalSize();
    }

    public OperationRunner getCurrentRunner() {
        return currentRunner;
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
        while (!shutdown) {
            Object task;
            try {
                task = queue.take();
            } catch (InterruptedException e) {
                continue;
            }

            processedTotal.inc();

            process(task);
        }
    }

    void process(Object task) {
        if (task.getClass() == Packet.class) {
            processPacket((Packet) task);
        } else if (task instanceof Operation) {
            processOperation((Operation) task);
        } else if (task instanceof PartitionSpecificRunnable) {
            processPartitionSpecificRunnable((PartitionSpecificRunnable) task);
        } else if (task instanceof Runnable) {
            processRunnable((Runnable) task);
        } else {
            throw new IllegalStateException("Unhandled task type for task:" + task);
        }
    }

    private void processPacket(Packet packet) {
        processedPackets.inc();

        try {
            currentRunner = getOperationRunner(packet.getPartitionId());
            currentRunner.run(packet);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process packet: " + packet + " on " + getName(), e);
        } finally {
            currentRunner = null;
        }
    }

    private void processOperation(Operation operation) {
        processedOperations.inc();

        try {
            currentRunner = getOperationRunner(operation.getPartitionId());
            currentRunner.run(operation);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process operation: " + operation + " on " + getName(), e);
        } finally {
            currentRunner = null;
        }
    }

    private void processPartitionSpecificRunnable(PartitionSpecificRunnable runnable) {
        processedPartitionSpecificRunnables.inc();

        try {
            currentRunner = getOperationRunner(runnable.getPartitionId());
            currentRunner.run(runnable);
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process task: " + runnable + " on " + getName(), e);
        } finally {
            currentRunner = null;
        }
    }

    private void processRunnable(Runnable runnable) {
        processedRunnables.inc();

        try {
            runnable.run();
        } catch (Throwable e) {
            inspectOutputMemoryError(e);
            logger.severe("Failed to process task: " + runnable + " on " + getName(), e);
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

    public int getThreadId() {
        return threadId;
    }
}
