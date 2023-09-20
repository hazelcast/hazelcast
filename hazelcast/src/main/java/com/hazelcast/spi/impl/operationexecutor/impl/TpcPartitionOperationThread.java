/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;


/**
 * The {@link TpcPartitionOperationThread} subclasses the {@link PartitionOperationThread} and
 * overrides the loop method. In the original PartitionOperationThread, there is a loop that
 * takes items from the operation queue and process them. With the TpcPartitionOperationThread
 * the loop method forwards to the eventloopTask. The eventloopTask loops over even sources (like
 * Nio Selectors) and other queues including the OperationQueue. With the TpcPartitionOperationThread
 * the thread blocks on the OperationQueue with a take. With the TPC version, it will only poll
 * and block on the Reactor (which in Nio blocks on the selector.select).
 */
public class TpcPartitionOperationThread extends PartitionOperationThread {

    private Runnable eventloopTask;

    public TpcPartitionOperationThread(String name,
                                       int threadId,
                                       TpcOperationQueue queue,
                                       ILogger logger,
                                       NodeExtension nodeExtension,
                                       OperationRunner[] partitionOperationRunners,
                                       ClassLoader configClassLoader) {
        super(name, threadId, queue, logger, nodeExtension, partitionOperationRunners, configClassLoader);
    }

    public TpcOperationQueue getQueue() {
        return (TpcOperationQueue) queue;
    }

    public void setEventloopTask(Runnable eventloopTask) {
        this.eventloopTask = eventloopTask;
    }

    @Override
    protected void loop() throws Exception {
        eventloopTask.run();
    }
}
