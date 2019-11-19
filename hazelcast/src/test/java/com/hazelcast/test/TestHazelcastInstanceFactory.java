/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.NodeContext;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.mocknetwork.TestNodeRegistry;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.impl.TestUtil.terminateInstance;
import static com.hazelcast.test.HazelcastTestSupport.getAddress;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

public class TestHazelcastInstanceFactory {
    private static final int DEFAULT_INITIAL_PORT = NetworkConfig.DEFAULT_PORT;
    private static final int MAX_PORT_NUMBER = (1 << 16) - 1;

    protected final TestNodeRegistry registry;

    private final boolean isMockNetwork = TestEnvironment.isMockNetwork();
    private final ConcurrentMap<Integer, Address> addressMap = new ConcurrentHashMap<Integer, Address>();
    private final AtomicInteger nodeIndex = new AtomicInteger();

    private final int count;

    public TestHazelcastInstanceFactory() {
        this(0);
    }

    public TestHazelcastInstanceFactory(int initialPort, String... addresses) {
        fillAddressMap(initialPort, addresses);
        this.count = addresses.length;
        this.registry = isMockNetwork ? createRegistry() : null;
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this(-1, addresses);
    }

    public TestHazelcastInstanceFactory(int count) {
        fillAddressMap(count);
        this.count = count;
        this.registry = isMockNetwork ? createRegistry() : null;
    }

    protected TestNodeRegistry createRegistry() {
        return new TestNodeRegistry(getKnownAddresses(), DefaultNodeContext.EXTENSION_PRIORITY_LIST);
    }

    public int getCount() {
        return count;
    }

    /**
     * Delegates to {@link #newHazelcastInstance(Config) {@code newHazelcastInstance(null)}}.
     */
    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance((Config) null);
    }

    /**
     * Returns the address with the given {@code host} and {@code port}.
     * This method may return {@code null} if no IP address for the {@code host}
     * could be found.
     */
    public static Address createAddressOrNull(String host, int port) {
        try {
            return new Address(host, port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return null;
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
        if (isMockNetwork) {
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
        if (isMockNetwork) {
            config = initOrCreateConfig(config);
            Address thisAddress = address != null ? address : nextAddress(config.getNetworkConfig().getPort());
            NodeContext nodeContext = registry.createNodeContext(thisAddress,
                    blockedAddresses == null
                            ? Collections.<Address>emptySet()
                            : new HashSet<Address>(asList(blockedAddresses)));
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        throw new UnsupportedOperationException("Explicit address is only available for mock network setup!");
    }

    /**
     * Asserts that the array and all of its elements are non-null.
     *
     * @param array the array
     * @param <T>   the type of the array elements
     * @throws NullPointerException if the array or any of its elements are {@code null}.
     */
    private static <T> void checkElementsNotNull(T[] array) {
        checkNotNull(array, "Array should not be null");
        for (Object element : array) {
            checkNotNull(element, "Array element should not be null");
        }
    }

    /**
     * Creates a new test Hazelcast instance.
     *
     * @param config the config to use; use {@code null} to get the default config
     */
    public HazelcastInstance newHazelcastInstance(Config config) {
        String instanceName = config != null ? config.getInstanceName() : null;
        if (isMockNetwork) {
            config = initOrCreateConfig(config);
            NodeContext nodeContext = registry.createNodeContext(nextAddress(config.getNetworkConfig().getPort()));
            return HazelcastInstanceFactory.newHazelcastInstance(config, instanceName, nodeContext);
        }
        return HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    public Address nextAddress() {
        return nextAddress(DEFAULT_INITIAL_PORT);
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
        if (isMockNetwork) {
            return registry.getAllHazelcastInstances();
        }
        return Hazelcast.getAllHazelcastInstances();
    }

    public Collection<Address> getKnownAddresses() {
        return unmodifiableCollection(addressMap.values());
    }

    public Address nextAddress(int initialPort) {
        int id = nodeIndex.getAndIncrement();

        Address currentAddress = addressMap.get(id);
        if (currentAddress != null) {
            return currentAddress;
        }

        return addNewAddressToAddressMap(id, "127.0.0.1", initialPort);
    }

    public TestNodeRegistry getRegistry() {
        return registry;
    }

    public HazelcastInstance getInstance(Address address) {
        if (isMockNetwork) {
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
        if (isMockNetwork) {
            registry.removeInstance(address);
        }
    }

    /**
     * Shutdown all instances started by this factory.
     */
    public void shutdownAll() {
        if (isMockNetwork) {
            registry.shutdown();
            addressMap.clear();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    /**
     * Terminates all instances started by this factory.
     */
    public void terminateAll() {
        if (isMockNetwork) {
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    @Override
    public String toString() {
        return "TestHazelcastInstanceFactory{addresses=" + addressMap.values() + '}';
    }

    private Address addNewAddressToAddressMap(int id, String host, int initialPort) {
        synchronized (addressMap) {
            while (true) {
                int newPort = initialPort++;

                if (newPort > MAX_PORT_NUMBER) {
                    throw new IllegalArgumentException(
                            "Exhausted available port range. Try lowering the initial port in "
                                    + getClass().getSimpleName() + ": " + newPort);
                }

                final Address newAddress = createAddressOrNull(host, newPort);
                if (!addressMap.containsValue(newAddress)) {
                    addressMap.put(id, newAddress);
                    return newAddress;
                }
            }
        }
    }

    /**
     * Returns a list of addresses with the {@code 127.0.0.1} host and starting
     * with the {@value DEFAULT_INITIAL_PORT} port or an empty list in case mock
     * network is not used or the requested count is {@code 0}.
     *
     * @param count the number of requested addresses
     */
    private void fillAddressMap(int count) {
        final String[] addresses = new String[count];
        Arrays.fill(addresses, "127.0.0.1");
        fillAddressMap(DEFAULT_INITIAL_PORT, addresses);
    }

    /**
     * Fills the {@link #addressMap} with a list of addresses with the provided
     * hosts and and starting with the provided {@code initialPort} port.
     *
     * @param initialPort the initial port for the returned addresses
     * @param hostArray   the array with the address hostnames
     */
    private void fillAddressMap(int initialPort, String... hostArray) {
        if (!isMockNetwork || hostArray.length == 0) {
            return;
        }
        checkElementsNotNull(hostArray);

        int port = initialPort == -1 ? DEFAULT_INITIAL_PORT : initialPort;
        int nodeIndex = 0;
        for (String host : hostArray) {
            addNewAddressToAddressMap(nodeIndex++, host, port++);
        }
    }

    public static Config initOrCreateConfig(Config config) {
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

    /**
     * Removes hazelcast instance address mappings for shut down hazelcast
     * instances. This allows an address to be reused.
     */
    public void cleanup() {
        if (!isMockNetwork) {
            return;
        }
        final TestNodeRegistry registry = getRegistry();
        synchronized (addressMap) {
            final Iterator<Entry<Integer, Address>> addressIterator = addressMap.entrySet().iterator();
            while (addressIterator.hasNext()) {
                final Entry<Integer, Address> entry = addressIterator.next();
                if (registry.getInstance(entry.getValue()) == null) {
                    addressIterator.remove();
                }
            }
        }
    }
}
