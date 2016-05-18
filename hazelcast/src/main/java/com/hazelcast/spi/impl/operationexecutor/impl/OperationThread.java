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

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.NodeExtension;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
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

    @Probe
    protected final SwCounter completedPacketCount = newSwCounter();
    @Probe
    protected final SwCounter completedOperationCount = newSwCounter();
    @Probe
    protected final SwCounter completedPartitionSpecificRunnableCount = newSwCounter();
    @Probe
    protected final SwCounter completedRunnableCount = newSwCounter();
    @Probe
    protected final SwCounter errorCount = newSwCounter();

    protected final ILogger logger;

    protected final NodeExtension nodeExtension;
    protected volatile boolean shutdown;

    public OperationThread(String name,
                           int threadId,
                           OperationQueue queue,
                           ILogger logger,
                           HazelcastThreadGroup threadGroup,
                           NodeExtension nodeExtension) {
        super(threadGroup.getInternalThreadGroup(), name);
        setContextClassLoader(threadGroup.getClassLoader());
        this.queue = queue;
        this.threadId = threadId;
        this.logger = logger;
        this.nodeExtension = nodeExtension;
    }

    @Override
    public final void run() {
        nodeExtension.onThreadStart(this);
        try {
            run0();
        } catch (Throwable t) {
            inspectOutOfMemoryError(t);
            logger.severe(t);
        } finally {
            nodeExtension.onThreadStop(this);
        }
    }

//    private void process(Object task) {
//        try {
//            if (task.getClass() == Packet.class) {
//                Packet packet = (Packet) task;
//                currentRunner = getOperationRunner(packet.getPartitionId());
//                currentRunner.run(packet);
//                completedPacketCount.inc();
//            } else if (task instanceof Operation) {
//                Operation operation = (Operation) task;
//                currentRunner = getOperationRunner(operation.getPartitionId());
//                currentRunner.run(operation);
//                completedOperationCount.inc();
//            } else if (task instanceof PartitionSpecificRunnable) {
//                PartitionSpecificRunnable runnable = (PartitionSpecificRunnable) task;
//                currentRunner = getOperationRunner(runnable.getPartitionId());
//                currentRunner.run(runnable);
//                completedPartitionSpecificRunnableCount.inc();
//            } else if (task instanceof Runnable) {
//                Runnable runnable = (Runnable) task;
//                runnable.run();
//                completedRunnableCount.inc();
//            } else {
//                throw new IllegalStateException("Unhandled task type for task:" + task);
//            }
//            completedTotalCount.inc();
//        } catch (Throwable t) {
//            errorCount.inc();
//            inspectOutOfMemoryError(t);
//            logger.severe("Failed to process packet: " + task + " on " + getName(), t);
//        } finally {
//            currentRunner = null;
//        }
    protected abstract void run0();

    public final int getThreadId() {
        return threadId;
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        registry.scanAndRegister(this, "operation.thread[" + getName() + "]");
    }

    @Probe
    private long completedTotalCount() {
        return completedPacketCount.get()
                + completedOperationCount.get()
                + completedPartitionSpecificRunnableCount.get()
                + completedRunnableCount.get();
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
