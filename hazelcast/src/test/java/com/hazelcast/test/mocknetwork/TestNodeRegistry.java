/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class TestNodeRegistry {

    private final ConcurrentMap<Address, NodeEngineImpl> nodes = new ConcurrentHashMap<Address, NodeEngineImpl>(10);
    private final Object joinerLock = new Object();
    private final Collection<Address> joinAddresses;

    public TestNodeRegistry(Collection<Address> addresses) {
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
        shutdown(false);
    }

    public void terminate() {
        shutdown(true);
    }

    private void shutdown(boolean terminate) {
        Iterator<NodeEngineImpl> iterator = nodes.values().iterator();
        while (iterator.hasNext()) {
            NodeEngineImpl nodeEngine = iterator.next();
            HazelcastInstance hz = nodeEngine.getHazelcastInstance();
            LifecycleService lifecycleService = hz.getLifecycleService();
            if (terminate) {
                lifecycleService.terminate();
            } else {
                lifecycleService.shutdown();
            }
            iterator.remove();
        }
    }

    ConcurrentMap<Address, NodeEngineImpl> getNodes() {
        return nodes;
    }

}
