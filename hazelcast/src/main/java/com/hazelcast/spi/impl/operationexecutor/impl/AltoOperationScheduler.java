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

import com.hazelcast.internal.tpc.Eventloop;
import com.hazelcast.internal.tpc.Scheduler;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * The Scheduler for Alto. So each reactor contains an partition-operation thread
 * and each of these threads runs an eventloop which contains a scheduler. This
 * scheduler is given a tick on every run of the eventloop to do some work. In
 * case of the alto, we process of a batch of operations from the operation-queue
 * and then hand control back to the eventloop.
 */
public class AltoOperationScheduler implements Scheduler {

    private final int batchSize;
    private AltoPartitionOperationThread operationThread;
    private OperationQueue queue;

    public AltoOperationScheduler(int batchSize) {
        this.batchSize = checkPositive("batchSize", batchSize);
    }

    @Override
    public void init(Eventloop eventloop) {
        // This method is guaranteed to be called from the Reactor thread (whic is the AltoPartitionOperationThread).
        this.operationThread = (AltoPartitionOperationThread) Thread.currentThread();
        this.queue = operationThread.queue;
    }

    @Override
    public boolean tick() {
        final AltoPartitionOperationThread operationThread = this.operationThread;
        final OperationQueue queue = this.queue;
        final int batchSize = this.batchSize;

        for (int k = 0; k < batchSize; k++) {
            if (operationThread.isShutdown()) {
                return false;
            }

            Object task = queue.poll();
            if (task == null) {
                return false;
            }

            operationThread.process(task);
        }

        return !queue.isEmpty();
    }

    @Override
    public void schedule(Object task) {
        // the OperationExecutor moves tasks directly into the OperationQueue.
        throw new UnsupportedOperationException();
    }
}
