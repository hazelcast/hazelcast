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

package com.hazelcast.spi.impl.operationexecutor.impl;


import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.tpc.CrappyThread;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;

import java.util.concurrent.CompletableFuture;


/**
 * The {@link AltoPartitionOperationThread} subclasses the {@link PartitionOperationThread} and
 * overrides the loop method. In the original PartitionOperationThread, there is a loop that
 * takes items from the operation queue and process them. With the AltoPartitionOperationThread
 * the loop method forwards to the loopTask. The loopTasks loops over even sources (like
 * Nio Selectors) and other queues including the OperationQueue. With the AltoPartitionOperationThread
 * the thread blocks on the OperationQueue with a take. With the alto version, it will only poll
 * and block on the Reactor (which in Nio blocks on the selector.select).
 */
public class AltoPartitionOperationThread extends PartitionOperationThread implements CrappyThread {

    private final CompletableFuture<Runnable> eventloopFuture = new CompletableFuture<>();

    public AltoPartitionOperationThread(String name,
                                        int threadId,
                                        AltoOperationQueue queue,
                                        ILogger logger,
                                        NodeExtension nodeExtension,
                                        OperationRunner[] partitionOperationRunners,
                                        ClassLoader configClassLoader) {
        super(name, threadId, queue, logger, nodeExtension, partitionOperationRunners, configClassLoader);
    }

    public AltoOperationQueue getQueue(){
        return (AltoOperationQueue) queue;
    }

    @Override
    public void setEventloopTask(Runnable eventloopTask) {
        eventloopFuture.complete(eventloopTask);
    }

    @Override
    protected void loop() throws Exception {
        Runnable eventloopTask = eventloopFuture.get();
        eventloopTask.run();
    }
}
