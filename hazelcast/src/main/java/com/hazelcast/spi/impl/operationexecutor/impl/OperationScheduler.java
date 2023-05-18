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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Scheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.function.Consumer;

import static com.hazelcast.internal.util.Preconditions.checkPositive;

/**
 * The Scheduler for TPC. So each reactor contains an partition-operation thread
 * and each of these threads runs an eventloop which contains a scheduler. This
 * scheduler is given a tick on every run of the eventloop to do some work. In
 * case of the TPC, we process of a batch of operations from the operation-queue
 * and then hand control back to the eventloop.
 */
public class OperationScheduler implements Scheduler {

    private final int batchSize;
    private Consumer<Packet> packetDispatcher;
    private TpcPartitionOperationThread operationThread;
    private OperationQueue queue;
    private Node node;

    public OperationScheduler(int batchSize, Node node) {
        this.batchSize = checkPositive("batchSize", batchSize);
        this.node = node;
    }

    @Override
    public void init(Eventloop eventloop) {
        // This method is guaranteed to be called from the Reactor thread (which
        // is the TpcPartitionOperationThread).
        this.operationThread = (TpcPartitionOperationThread) Thread.currentThread();
        this.queue = operationThread.queue;
    }

    @Override
    public boolean tick() {
        final TpcPartitionOperationThread operationThread0 = operationThread;
        final OperationQueue queue0 = queue;
        final int batchSize0 = batchSize;

        for (int k = 0; k < batchSize0; k++) {
            if (operationThread0.isShutdown()) {
                return false;
            }

            Object task = queue0.poll();
            if (task == null) {
                return false;
            }

            operationThread0.process(task);
        }

        return !queue0.isEmpty();
    }

    @Override
    public void schedule(IOBuffer task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void schedule(Object task) {
        if (task instanceof Operation) {
            Operation op = (Operation) task;
            queue.add(op, op.isUrgent());
        } else if (task instanceof Packet) {
            if (packetDispatcher == null) {
                packetDispatcher = node.nodeEngine.getPacketDispatcher();
            }
            packetDispatcher.accept((Packet) task);
        } else {
            throw new UnsupportedOperationException();
        }
    }
}
