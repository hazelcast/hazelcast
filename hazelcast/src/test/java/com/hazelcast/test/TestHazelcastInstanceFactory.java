/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static java.util.Collections.EMPTY_SET;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableCollection;

public class TestHazelcastInstanceFactory {

    protected final TestNodeRegistry registry;

    private static final AtomicInteger PORTS = new AtomicInteger(5000);

    private final ConcurrentMap<Integer, Address> addressMap = new ConcurrentHashMap<Integer, Address>();
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final AtomicInteger nodeIndex = new AtomicInteger();

    private final int count;

    public TestHazelcastInstanceFactory() {
        this(0);
    }

    public TestHazelcastInstanceFactory(int count) {
        this.count = count;
        this.registry = getRegistry(createAddresses(PORTS, count));
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this(-1, addresses);
    }

    public TestHazelcastInstanceFactory(int initialPort, String... addresses) {
        this.count = addresses.length;
        this.registry = getRegistry(createAddresses(initialPort, PORTS, addresses));
    }

    public TestHazelcastInstanceFactory(Collection<Address> addresses) {
        this.count = addresses.size();
        this.registry = getRegistry(addresses);
    }

    private TestNodeRegistry getRegistry(Collection<Address> addresses) {
        if (!mockNetwork) {
            return null;
        }

        int ix = 0;
        for (Address address : addresses) {
            addressMap.put(ix++, address);
        }
        return new TestNodeRegistry(getKnownAddresses());
    }

    /**
     * Delegates to {@link #newHazelcastInstance(Config) {@code newHazelcastInstance(null)}}.
     */
    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance((Config) null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param config the config to use; use {@code null} to get the default config
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            config = initOrCreateConfig(config);
            NodeContext nodeContext = registry.createNodeContext(nextAddress());
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        return HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param address the address to use as Member's address instead of picking the next address
     */
    public HazelcastInstance newHazelcastInstance(Address address) {
        return newHazelcastInstance(address, null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param address the address to use as Member's address instead of picking the next address
     * @param config  the config to use; use {@code null} to get the default config
     */
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        final String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            config = initOrCreateConfig(config);
            NodeContext nodeContext = registry.createNodeContext(address);
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        throw new UnsupportedOperationException("Explicit address is only available for mock network setup!");
    }

    /**
     * Creates a new test Hazelcast instance which is only allowed to connect to specified addresses:
     * <ul>
     * <li>{@code blockedAddresses} are blacklisted in its {@code MockJoiner}</li>
     * <li>connections to {@code blockedAddresses} are blocked by its {@code FirewallingConnectionManager}</li>
     * </ul>
     * This is handy in split-brain tests, when a new instance should be started on a specific network partition
     * of the split brain.
     *
     * @param config           the config to use; use {@code null} to get the default config
     * @param blockedAddresses addresses to which the new instance is allowed to communicate
     */
    public HazelcastInstance newHazelcastInstance(Config config, Address[] blockedAddresses) {
        return newHazelcastInstance(null, config, blockedAddresses);
    }

    /**
     * Creates a new test Hazelcast instance which is only allowed to connect to specified addresses:
     * <ul>
     * <li>{@code blockedAddresses} are blacklisted in its {@code MockJoiner}</li>
     * <li>connections to {@code blockedAddresses} are blocked by its {@code FirewallingConnectionManager}</li>
     * </ul>
     * This is handy in split-brain tests, when a new instance should be started on a specific network partition
     * of the split brain.
     *
     * @param address          the address to use as Member's address; if {@code null}, then uses the next address
     * @param config           the config to use; use {@code null} to get the default config
     * @param blockedAddresses addresses to which the new instance is allowed to communicate
     */
    public HazelcastInstance newHazelcastInstance(Address address, Config config, Address[] blockedAddresses) {
        final String instanceName = config != null ? config.getInstanceName() : null;
        final Address thisAddress = address != null ? address : nextAddress();
        if (mockNetwork) {
            config = initOrCreateConfig(config);
            NodeContext nodeContext = registry.createNodeContext(thisAddress,
                    blockedAddresses == null ? EMPTY_SET : new HashSet<Address>(Arrays.asList(blockedAddresses)));
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        throw new UnsupportedOperationException("Explicit address is only available for mock network setup!");
    }

    public Address nextAddress() {
        int id = nodeIndex.getAndIncrement();

        Address currentAddress = addressMap.get(id);
        if (currentAddress != null) {
            return currentAddress;
        }

        Address newAddress = createAddress("127.0.0.1", PORTS.incrementAndGet());
        addressMap.put(id, newAddress);
        return newAddress;
    }

    public HazelcastInstance[] newInstances() {
        return newInstances(new Config());
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
        for (int i = 0; i < nodeCount; i++) {
            instances[i] = newHazelcastInstance(config);
        }
        return instances;
    }

    public HazelcastInstance[] newInstances(Config config) {
        return newInstances(config, count);
    }

    public Collection<HazelcastInstance> getAllHazelcastInstances() {
        if (mockNetwork) {
            return registry.getAllHazelcastInstances();
        }
        return Hazelcast.getAllHazelcastInstances();
    }

    public Collection<Address> getKnownAddresses() {
        return unmodifiableCollection(addressMap.values());
    }

    public TestNodeRegistry getRegistry() {
        return registry;
    }

    public HazelcastInstance getInstance(Address address) {
        if (mockNetwork) {
            return registry.getInstance(address);
        }

        Set<HazelcastInstance> instances = HazelcastInstanceFactory.getAllHazelcastInstances();
        for (HazelcastInstance instance : instances) {
            if (address.equals(getAddress(instance))) {
                return instance;
            }
        }
        return null;
    }

    /**
     * Terminates supplied instance by releasing internal resources.
     *
     * @param instance the instance to terminate
     */
    public void terminate(HazelcastInstance instance) {
        Address address = getNode(instance).address;
        terminateInstance(instance);
        registry.removeInstance(address);
    }

    public void shutdownAll() {
        if (mockNetwork) {
            registry.shutdown();
            addressMap.clear();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public void terminateAll() {
        if (mockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    @Override
    public String toString() {
        return "TestHazelcastInstanceFactory{addresses=" + addressMap.values() + '}';
    }

    private List<Address> createAddresses(AtomicInteger ports, int count) {
        if (!mockNetwork || count == 0) {
            return emptyList();
        }
        List<Address> addresses = new ArrayList<Address>(count);
        for (int i = 0; i < count; i++) {
            addresses.add(createAddress("127.0.0.1", ports.incrementAndGet()));
        }
        return addresses;
    }

    private List<Address> createAddresses(int initialPort, AtomicInteger ports, String... addressArray) {
        if (!mockNetwork) {
            return emptyList();
        }
        checkElementsNotNull(addressArray);

        int count = addressArray.length;
        List<Address> addresses = new ArrayList<Address>(count);
        for (String address : addressArray) {
            int port = initialPort == -1 ? ports.incrementAndGet() : initialPort++;
            addresses.add(createAddress(address, port));
        }
        return addresses;
    }

    private static Address createAddress(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static <T> void checkElementsNotNull(T[] array) {
        checkNotNull(array, "Array should not be null");
        for (Object element : array) {
            checkNotNull(element, "Array element should not be null");
        }
    }

    private static Config initOrCreateConfig(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        String gracefulShutdownMaxWaitValue = System.getProperty(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), "120");
        config.setProperty(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), gracefulShutdownMaxWaitValue);
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    public int getCount() {
        return count;
    }
}
