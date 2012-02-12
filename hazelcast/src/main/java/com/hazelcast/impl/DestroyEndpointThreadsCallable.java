/*
 * Copyright (c) 2008-2012, Hazel Bilisim Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

public class DestroyEndpointThreadsCallable implements Callable<Boolean>, HazelcastInstanceAware, DataSerializable {

    private transient HazelcastInstance hazelcastInstance;
    private Address endpoint;
    private Set<Integer> threadIds;

    public DestroyEndpointThreadsCallable() {
    }

    public DestroyEndpointThreadsCallable(Address endpoint, Set<Integer> threadIds) {
        this.endpoint = endpoint;
        this.threadIds = threadIds;
    }

    public void addThreadId(int threadId) {
        threadIds.add(threadId);
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public Boolean call() throws Exception {
        final ConcurrentMapManager c = getConcurrentMapManager(hazelcastInstance);
        c.enqueueAndWait(new Processable() {
            public void process() {
                c.destroyEndpointThreads(endpoint, threadIds);
            }
        }, 10);
        return Boolean.TRUE;
    }

    private ConcurrentMapManager getConcurrentMapManager(HazelcastInstance h) {
        FactoryImpl factory = (FactoryImpl) h;
        return factory.node.concurrentMapManager;
    }

    public void writeData(DataOutput out) throws IOException {
        endpoint.writeData(out);
        int size = threadIds.size();
        out.writeInt(size);
        for (Integer threadId : threadIds) {
            out.writeInt(threadId);
        }
    }

    public void readData(DataInput in) throws IOException {
        endpoint = new Address();
        endpoint.readData(in);
        int size = in.readInt();
        threadIds = new HashSet<Integer>(size);
        for (int i = 0; i < size; i++) {
            addThreadId(in.readInt());
        }
    }
}
