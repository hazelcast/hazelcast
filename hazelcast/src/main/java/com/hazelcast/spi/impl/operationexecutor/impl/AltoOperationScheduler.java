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

public class AltoOperationScheduler implements Scheduler {

    public static final int BATCH_SIZE = 10;
    private Eventloop eventloop;
    private AltoPartitionOperationThread operationThread;
    private OperationQueue queue;

    @Override
    public void init(Eventloop eventloop) {
        this.eventloop = eventloop;
        this.operationThread = (AltoPartitionOperationThread) Thread.currentThread();
        this.queue = operationThread.queue;
    }

    @Override
    public boolean tick() {
        final AltoPartitionOperationThread operationThread = this.operationThread;
        final OperationQueue queue = this.queue;

        for (int k = 0; k < BATCH_SIZE; k++) {
            if (operationThread.isShutdown()) {
                return false;
            }

            Object task = queue.poll();
            if (task == null) {
                return false;
            }

            operationThread.process(task);
        }

        return queue.remaining();
    }

    @Override
    public void schedule(Object task) {
        // todo: not used.
    }
}
