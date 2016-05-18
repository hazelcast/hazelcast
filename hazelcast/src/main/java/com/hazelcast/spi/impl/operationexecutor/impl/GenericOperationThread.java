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
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

import static com.hazelcast.instance.OutOfMemoryErrorDispatcher.inspectOutputMemoryError;
import static sun.management.snmp.jvminstr.JvmThreadInstanceEntryImpl.ThreadStateMap.Byte0.runnable;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    final OperationRunner operationRunner;
    private final boolean priority;

    public GenericOperationThread(String name, int threadId, OperationQueue queue, ILogger logger,
                                  HazelcastThreadGroup threadGroup, NodeExtension nodeExtension,
                                  OperationRunner operationRunner, boolean priority) {
        super(name, threadId, queue, logger, threadGroup, nodeExtension);
        this.operationRunner = operationRunner;
        this.priority = priority;
    }

    @Override
    protected void run0() {
        while (!shutdown) {
            Object task;
            try {
                task = queue.take(priority);
            } catch (InterruptedException e) {
                continue;
            }

            try {
                if (task.getClass() == Packet.class) {
                    process((Packet)task);
                } else if (task instanceof Operation) {
                    process((Operation) task);
                } else if (task instanceof PartitionSpecificRunnable) {
                    process((PartitionSpecificRunnable)task);
                } else if (task instanceof Runnable) {
                    process((Runnable) task);
                } else {
                    throw new IllegalStateException("Unhandled task type for task:" + task);
                }
            } catch (Throwable t) {
                errorCount.inc();
                inspectOutputMemoryError(t);
                logger.severe("Failed to process: " + task + " on " + getName(), t);
            }
        }
    }

    private void process(Runnable task) {
        task.run();
        completedRunnableCount.inc();
    }

    private void process(PartitionSpecificRunnable task) {
        operationRunner.run(task);
        completedPartitionSpecificRunnableCount.inc();
    }

    private void process(Operation operation) {
        operationRunner.run(operation);
        completedOperationCount.inc();
    }

    private void process(Packet packet) throws Exception {
        operationRunner.run(packet);
        completedPacketCount.inc();
    }
}
