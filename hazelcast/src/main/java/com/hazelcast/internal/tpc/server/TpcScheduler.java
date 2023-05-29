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

package com.hazelcast.internal.tpc.server;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.tpcengine.Eventloop;
import com.hazelcast.internal.tpcengine.Scheduler;
import com.hazelcast.internal.tpcengine.iobuffer.IOBuffer;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationScheduler;

import java.util.Queue;

/**
 * The TpcScheduler effectively is chain 2 schedulers:
 * - The OpScheduler for next generation operations
 * - The OperationScheduler for classic operations.
 */
public class TpcScheduler implements Scheduler {

    private final RequestScheduler requestScheduler;
    private final OperationScheduler operationScheduler;

    public TpcScheduler(RequestScheduler requestScheduler, OperationScheduler operationScheduler) {
        this.requestScheduler = requestScheduler;
        this.operationScheduler = operationScheduler;
    }

    @Override
    public Queue queue() {
        return null;
    }

    @Override
    public void init(Eventloop eventloop) {
        operationScheduler.init(eventloop);
        requestScheduler.init(eventloop);
    }

    @Override
    public boolean tick() {
        boolean hasMore = false;
        hasMore |= requestScheduler.tick();
        hasMore |= operationScheduler.tick();
        return hasMore;
    }

    @Override
    public void schedule(IOBuffer task) {
        requestScheduler.schedule(task);
    }

    @Override
    public void schedule(Object task) {
        if (task instanceof Packet) {
            operationScheduler.schedule(task);
        } else {
            requestScheduler.schedule(task);
        }
    }
}
