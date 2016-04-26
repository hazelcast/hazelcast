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
 */

package com.hazelcast.test;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceManager;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.TestUtil.getNode;
import static com.hazelcast.instance.TestUtil.terminateInstance;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.util.Preconditions.checkNotNull;

public class TestHazelcastInstanceFactory {

    private static final AtomicInteger PORTS = new AtomicInteger(5000);

    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final AtomicInteger nodeIndex = new AtomicInteger();

    protected TestNodeRegistry registry;
    protected final ConcurrentMap<Integer, Address> addressMap = new ConcurrentHashMap<Integer, Address>();
    private final int count;

    public TestHazelcastInstanceFactory(int count) {
        this.count = count;
        if (mockNetwork) {
            List<Address> addresses = createAddresses(PORTS, count);
            initFactory(addresses);
        }
    }

    public TestHazelcastInstanceFactory() {
        this.count = 0;
        this.registry = new TestNodeRegistry(addressMap.values());
    }

    public TestHazelcastInstanceFactory(int initialPort, String... addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            List<Address> addressList = createAddresses(initialPort, PORTS, addresses);
            initFactory(addressList);
        }
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            List<Address> addressesList = createAddresses(-1, PORTS, addresses);
            initFactory(addressesList);
        }
    }

    public TestHazelcastInstanceFactory(Collection<Address> addresses) {
        this.count = addresses.size();
        if (mockNetwork) {
            initFactory(addresses);
        }
    }

    private void initFactory(Collection<Address> addresses) {
        int ix = 0;
        for (Address address : addresses) {
            addressMap.put(ix++, address);
        }
        this.registry = new TestNodeRegistry(addressMap.values());
    }

    /**
     * Delegates to {@link #newHazelcastInstance(Config) <code>newHazelcastInstance(null)</code>}.
     */
    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance((Config) null);
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param config the config to use; use <code>null</code> to get the default config.
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            init(config);
            NodeContext nodeContext = registry.createNodeContext(pickAddress());
            return HazelcastInstanceManager.newHazelcastInstance(config, instanceName, nodeContext);
        }
        return HazelcastInstanceManager.newHazelcastInstance(config);
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
     * @param config  the config to use; use <code>null</code> to get the default config.
     */
    public HazelcastInstance newHazelcastInstance(Address address, Config config) {
        final String instanceName = config != null ? config.getInstanceName() : null;
        if (mockNetwork) {
            init(config);
            NodeContext nodeContext = registry.createNodeContext(address);
            return HazelcastInstanceManager.newHazelcastInstance(config, instanceName, nodeContext);
        }
        throw new UnsupportedOperationException("Explicit address is only available for mock network setup!");
    }

    private Address pickAddress() {
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

    /**
     * Terminates supplied instance by releasing internal resources.
     *
     * @param instance the instance.
     */
    public void terminate(HazelcastInstance instance) {
        Address address = getNode(instance).address;
        terminateInstance(instance);
        registry.removeInstance(address);
    }

    public HazelcastInstance getInstance(Address address) {
        if (mockNetwork) {
            return registry.getInstance(address);
        }

        Set<HazelcastInstance> instances = HazelcastInstanceManager.getAllHazelcastInstances();
        for (HazelcastInstance instance : instances) {
            if (address.equals(getAddress(instance))) {
                return instance;
            }
        }
        return null;
    }

    public void shutdownAll() {
        if (mockNetwork) {
            registry.shutdown();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public void terminateAll() {
        if (mockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceManager.terminateAll();
        }
    }

    private static List<Address> createAddresses(AtomicInteger ports, int count) {
        List<Address> addresses = new ArrayList<Address>(count);
        for (int i = 0; i < count; i++) {
            addresses.add(createAddress("127.0.0.1", ports.incrementAndGet()));
        }
        return addresses;
    }

    private static List<Address> createAddresses(int initialPort, AtomicInteger ports, String... addressArray) {
        checkElementsNotNull(addressArray);

        int count = addressArray.length;
        List<Address> addresses = new ArrayList<Address>(count);
        for (String address : addressArray) {
            int port = initialPort == -1 ? ports.incrementAndGet() : initialPort++;
            addresses.add(createAddress(address, port));
        }
        return addresses;
    }

    protected static Address createAddress(String host, int port) {
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

    private static Config init(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config.setProperty(GroupProperty.GRACEFUL_SHUTDOWN_MAX_WAIT.getName(), "120");
        config.setProperty(GroupProperty.PARTITION_BACKUP_SYNC_INTERVAL.getName(), "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    public Collection<Address> getKnownAddresses() {
        return Collections.unmodifiableCollection(addressMap.values());
    }

    @Override
    public String toString() {
        return "TestHazelcastInstanceFactory{addresses=" + addressMap.values() + '}';
    }

    public TestNodeRegistry getRegistry() {
        return registry;
    }
}
