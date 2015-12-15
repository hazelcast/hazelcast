/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 *
 */

package com.hazelcast.test.mocknetwork;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

public final class TestNodeRegistry {

    private final ConcurrentMap<Address, NodeEngineImpl> nodes = new ConcurrentHashMap<Address, NodeEngineImpl>(10);
    private final Object joinerLock = new Object();
    private final CopyOnWriteArrayList<Address> joinAddresses;

    public TestNodeRegistry(CopyOnWriteArrayList<Address> addresses) {
        this.joinAddresses = addresses;
    }

    public NodeContext createNodeContext(Address address) {
        NodeEngineImpl nodeEngine;
        if ((nodeEngine = nodes.get(address)) != null) {
            if (nodeEngine.isRunning()) {
                throw new IllegalArgumentException("This address already in registry! " + address);
            }
            nodes.remove(address);
        }
        return new MockNodeContext(joinAddresses, nodes, address, joinerLock);
    }

    public HazelcastInstance getInstance(Address address) {
        NodeEngineImpl nodeEngine = nodes.get(address);
        return nodeEngine != null && nodeEngine.isRunning() ? nodeEngine.getHazelcastInstance() : null;
    }

    public boolean removeInstance(Address address) {
        return nodes.remove(address) != null;
    }

    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        Collection<HazelcastInstance> all = new LinkedList<HazelcastInstance>();
        for (NodeEngineImpl nodeEngine : nodes.values()) {
            if (nodeEngine.isRunning()) {
                all.add(nodeEngine.getHazelcastInstance());
            }
        }
        return all;
    }

    public void shutdown() {
        Collection<NodeEngineImpl> values = new ArrayList<NodeEngineImpl>(nodes.values());
        nodes.clear();
        for (NodeEngineImpl value : values) {
            value.getHazelcastInstance().shutdown();
        }
    }

    public void terminate() {
        Collection<NodeEngineImpl> values = new ArrayList<NodeEngineImpl>(nodes.values());
        nodes.clear();
        for (NodeEngineImpl value : values) {
            HazelcastInstance hz = value.getHazelcastInstance();
            hz.getLifecycleService().terminate();
        }
    }

    ConcurrentMap<Address, NodeEngineImpl> getNodes() {
        return nodes;
    }

}
