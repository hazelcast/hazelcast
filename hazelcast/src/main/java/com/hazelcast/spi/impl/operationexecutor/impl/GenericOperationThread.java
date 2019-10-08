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
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * An {@link OperationThread} for non partition specific operations.
 */
public final class GenericOperationThread extends OperationThread {

    private final OperationRunner operationRunner;

    public GenericOperationThread(String name,
                                  int threadId,
                                  OperationQueue queue,
                                  ILogger logger,
                                  NodeExtension nodeExtension,
                                  OperationRunner operationRunner,
                                  boolean priority,
                                  ClassLoader configClassLoader) {
        super(name, threadId, queue, logger, nodeExtension, priority, configClassLoader);
        this.operationRunner = operationRunner;
    }

    @Override
    public OperationRunner operationRunner(int partitionId) {
        return operationRunner;
    }

    @Override
    protected void process(Operation operation) {
        currentRunner = operationRunner(operation.getPartitionId());
        currentRunner.run(operation);
        completedOperationCount.inc();
    }

    @Override
    protected void process(Packet packet) throws Exception {
        int partitionId = packet.getPartitionId();
        //todo: if detect
        currentRunner = operationRunner(partitionId);
        currentRunner.run(packet);
        completedPacketCount.inc();
    }

    @Override
    protected void process(PartitionSpecificRunnable runnable) {
        currentRunner = operationRunner(runnable.getPartitionId());
        currentRunner.run(runnable);
        completedPartitionSpecificRunnableCount.inc();
    }
}
