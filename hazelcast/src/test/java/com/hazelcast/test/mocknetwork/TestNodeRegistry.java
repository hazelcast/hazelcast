/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.internal.util.AddressUtil;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.instance.impl.NodeState.SHUT_DOWN;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableMap;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class TestNodeRegistry {

    private final ConcurrentMap<Address, Node> nodes = new ConcurrentHashMap<>(10);
    private final Collection<Address> joinAddresses;

    public TestNodeRegistry(Collection<Address> addresses) {
        joinAddresses = addresses;
    }

    public NodeContext createNodeContext(final Address address, Set<Address> initiallyBlockedAddresses) {
        final Node node = nodes.get(address);
        if (node != null) {
            assertFalse(address + " is already registered", node.isRunning());
            assertTrueEventually(() ->
                    assertTrue(address + " should be SHUT_DOWN", node.getState() == SHUT_DOWN));
            nodes.remove(address, node);
        }
        return new MockNodeContext(this, address, initiallyBlockedAddresses);
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
        Collection<HazelcastInstance> all = new ArrayList<>();
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
        return unmodifiableMap(nodes);
    }

    Node getNode(Address address) {
        return nodes.get(address);
    }

    Collection<Address> getJoinAddresses() {
        return unmodifiableCollection(joinAddresses);
    }

    void registerNode(Node node) {
        Address address = node.getThisAddress();
        Node currentNode = nodes.putIfAbsent(address, node);
        assertTrue("This address is already in registry! " + address,
                currentNode == null || currentNode.equals(node));
    }

    Collection<Address> getAddresses() {
        return unmodifiableCollection(nodes.keySet());
    }
}
