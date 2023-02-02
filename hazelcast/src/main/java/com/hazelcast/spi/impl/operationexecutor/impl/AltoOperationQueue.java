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

import com.hazelcast.internal.tpc.Reactor;
import org.jctools.queues.MpmcArrayQueue;

public class AltoOperationQueue implements OperationQueue {

    public static final int CAPACITY = 1024;
    private Reactor reactor;
    private MpmcArrayQueue normalQueue = new MpmcArrayQueue(CAPACITY);
    private MpmcArrayQueue priorityQueue = new MpmcArrayQueue(CAPACITY);

    public AltoOperationQueue(Reactor reactor) {
        this.reactor = reactor;
    }

    @Override
    public void add(Object task, boolean priority) {
        if (priority) {
            priorityQueue.offer(task);
        } else {
            normalQueue.offer(task);
        }

        reactor.wakeup();
    }

    @Override
    public Object take(boolean priorityOnly) throws InterruptedException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int normalSize() {
        return normalQueue.size();
    }

    @Override
    public int prioritySize() {
        return priorityQueue.size();
    }

    @Override
    public int size() {
        return normalQueue.size() + priorityQueue.size();
    }

    @Override
    public Object poll() {
        Object item = priorityQueue.poll();
        if (item != null) {
            return item;
        } else {
            return normalQueue.poll();
        }
    }

    @Override
    public boolean remaining() {
        return !normalQueue.isEmpty() || !priorityQueue.isEmpty();
    }
}
