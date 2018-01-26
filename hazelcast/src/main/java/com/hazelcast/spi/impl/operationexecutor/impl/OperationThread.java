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

package com.hazelcast.spi.impl.operationexecutor.impl;

import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.util.executor.HazelcastManagedThread;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutOfMemoryError;
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
public abstract class OperationThread extends HazelcastManagedThread implements MetricsProvider {

    final int threadId;
    final OperationQueue queue;
    // This field wil only be accessed by the thread itself when doing 'self' calls. So no need
    // for any form of synchronization.
    OperationRunner currentRunner;

    // All these counters are updated by this OperationThread (so a single writer) and are read by the MetricsRegistry.
    @Probe
    private final SwCounter completedTotalCount = newSwCounter();
    @Probe
    private final SwCounter completedPacketCount = newSwCounter();
    @Probe
    private final SwCounter completedOperationCount = newSwCounter();
    @Probe
    private final SwCounter completedPartitionSpecificRunnableCount = newSwCounter();
    @Probe
    private final SwCounter completedRunnableCount = newSwCounter();
    @Probe
    private final SwCounter errorCount = newSwCounter();

    private final boolean priority;
    private final NodeExtension nodeExtension;
    private final ILogger logger;
    private volatile boolean shutdown;
    public OperationThread(String name, int threadId, OperationQueue queue, ILogger logger,
                           NodeExtension nodeExtension, boolean priority, ClassLoader configClassLoader) {
        super(name);
        setContextClassLoader(configClassLoader);
        this.queue = queue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
        this.priority = priority;
    }

    public int getThreadId() {
        return threadId;
    }

    public abstract OperationRunner getOperationRunner(int partitionId);

    @Override
    public final void run() {
        nodeExtension.onThreadStart(this);
        try {
            while (!shutdown) {
                Object task;
                try {
                    task = queue.take(priority);
                } catch (InterruptedException e) {
                    continue;
                }

                process(task);
            }
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe(t);
        } finally {
            nodeExtension.onThreadStop(this);
        }
    }

    private void process(Object task) {
        try {
            if (task.getClass() == Packet.class) {
                Packet packet = (Packet) task;
                currentRunner = getOperationRunner(packet.getPartitionId());
                currentRunner.run(packet);
                completedPacketCount.inc();
            } else if (task instanceof Operation) {
                Operation operation = (Operation) task;
                currentRunner = getOperationRunner(operation.getPartitionId());
                currentRunner.run(operation);
                completedOperationCount.inc();
            } else if (task instanceof PartitionSpecificRunnable) {
                PartitionSpecificRunnable runnable = (PartitionSpecificRunnable) task;
                currentRunner = getOperationRunner(runnable.getPartitionId());
                currentRunner.run(runnable);
                completedPartitionSpecificRunnableCount.inc();
            } else if (task instanceof Runnable) {
                Runnable runnable = (Runnable) task;
                runnable.run();
                completedRunnableCount.inc();
            } else {
                throw new IllegalStateException("Unhandled task type for task:" + task);
            }
            completedTotalCount.inc();
        } catch (Throwable t) {
            errorCount.inc();
            inspectOutOfMemoryError(t);
            logger.severe("Failed to process packet: " + task + " on " + getName(), t);
        } finally {
            currentRunner = null;
        }
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "operation.thread[" + getName() + "]");
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
