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

package com.hazelcast.test.mocknetwork;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.AssertTask;
import com.hazelcast.internal.util.AddressUtil;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class TestNodeRegistry {

    private final ConcurrentMap<Address, Node> nodes = new ConcurrentHashMap<>(10);
    private final Collection<Address> joinAddresses;
    private final List<String> nodeExtensionPriorityList;

    public TestNodeRegistry(Collection<Address> addresses, List<String> nodeExtensionPriorityList) {
        this.joinAddresses = addresses;
        this.nodeExtensionPriorityList = nodeExtensionPriorityList;
    }

    public NodeContext createNodeContext(Address address) {
        return createNodeContext(address, Collections.<Address>emptySet());
    }

    public NodeContext createNodeContext(final Address address, Set<Address> initiallyBlockedAddresses) {
        final Node node = nodes.get(address);
        if (node != null) {
            assertFalse(address + " is already registered", node.isRunning());
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() {
                    assertEquals(address + " should be SHUT_DOWN", NodeState.SHUT_DOWN, node.getState());
                }
            });
            nodes.remove(address, node);
        }
        return new MockNodeContext(this, address, initiallyBlockedAddresses, nodeExtensionPriorityList);
    }

    public HazelcastInstance getInstance(Address address) {
        Node node = nodes.get(address);
        if (node == null) {
            String host = address.getHost();
            if (host != null) {
                try {
                    if (AddressUtil.isIpAddress(host)) {
                        // try using hostname
                        node = nodes.get(new Address(address.getInetAddress().getHostName(), address.getPort()));
                    } else {
                        // try using ip address
                        node = nodes.get(new Address(address.getInetAddress().getHostAddress(), address.getPort()));
                    }
                } catch (UnknownHostException e) {
                    // suppress
                }
            }
        }
        return node != null && node.isRunning() ? node.hazelcastInstance : null;
    }

    public void removeInstance(Address address) {
        nodes.remove(address);
    }

    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        Collection<HazelcastInstance> all = new LinkedList<HazelcastInstance>();
        for (Node node : nodes.values()) {
            if (node.isRunning()) {
                all.add(node.hazelcastInstance);
            }
        }
        return all;
    }

    public UUID uuidOf(Address address) {
        Node node = nodes.get(address);
        return node != null ? node.getThisUuid() : null;
    }

    public Address addressOf(UUID memberUuid) {
        Optional<Address> memberAddress = nodes.entrySet()
                .stream()
                .filter(entry -> entry.getValue().getThisUuid() == memberUuid)
                .map(Map.Entry::getKey)
                .findAny();
        return memberAddress.orElse(null);
    }

    public void shutdown() {
        shutdown(false);
    }

    public void terminate() {
        shutdown(true);
    }

    private void shutdown(boolean terminate) {
        Iterator<Node> iterator = nodes.values().iterator();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            HazelcastInstance hz = node.hazelcastInstance;
            LifecycleService lifecycleService = hz.getLifecycleService();
            if (terminate) {
                lifecycleService.terminate();
            } else {
                lifecycleService.shutdown();
            }
            iterator.remove();
        }
    }

    Map<Address, Node> getNodes() {
        return Collections.unmodifiableMap(nodes);
    }

    Node getNode(Address address) {
        return nodes.get(address);
    }

    Collection<Address> getJoinAddresses() {
        return Collections.unmodifiableCollection(joinAddresses);
    }

    void registerNode(Node node) {
        Address address = node.getThisAddress();
        Node currentNode = nodes.putIfAbsent(address, node);
        if (currentNode != null) {
            verifyInvariant(currentNode.equals(node), "This address is already in registry! " + address);
        }
    }

    Collection<Address> getAddresses() {
        return Collections.unmodifiableCollection(nodes.keySet());
    }

    private static void verifyInvariant(boolean check, String msg) {
        if (!check) {
            throw new AssertionError(msg);
        }
    }
}
