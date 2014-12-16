/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.NodeContext;
import com.hazelcast.nio.Address;

import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.util.ValidationUtil.checkNotNull;

public final class TestHazelcastInstanceFactory {

    private final static String HAZELCAST_CLIENT = "com.hazelcast.client.HazelcastClient";
    private final static String HAZELCAST_CLIENT_CONFIG = "com.hazelcast.client.config.ClientConfig";
    private final static AtomicInteger PORTS = new AtomicInteger(5000);

    public final boolean mockNetwork = TestEnvironment.isMockNetwork();
    public final boolean useClient = TestEnvironment.isUseClient();

    private final Address[] addresses;
    private final TestNodeRegistry registry;
    private final AtomicInteger nodeIndex = new AtomicInteger();
    private final int count;

    public TestHazelcastInstanceFactory(int count) {
        this(createAddresses(count));
    }

    public TestHazelcastInstanceFactory(String... addresses) {
        this(createAddresses(addresses));
    }

    private TestHazelcastInstanceFactory(Address[] addresses) {
        this.count = addresses.length;
        if (mockNetwork) {
            this.addresses = addresses;
            this.registry = new TestNodeRegistry(this.addresses);
        } else {
            this.addresses = null;
            this.registry = null;
        }
    }

    public HazelcastInstance newHazelcastInstance() {
        return newHazelcastInstance(new Config());
    }

    public HazelcastInstance newHazelcastInstance(Config config) {
        if (mockNetwork) {
            if (nodeIndex.get() >= addresses.length) {
                throw new IndexOutOfBoundsException("Max " + addresses.length + " instances can be created!");
            }
            config = init(config);
            NodeContext nodeContext = registry.createNodeContext(addresses[nodeIndex.getAndIncrement()]);
            return HazelcastInstanceFactory.newHazelcastInstance(config, null, nodeContext);
        }
        if (useClient) {
            return newHazelcastClient();
        } else {
            return HazelcastInstanceFactory.newHazelcastInstance(config);
        }
    }

    public HazelcastInstance[] newInstances() {
        return newInstances(new Config());
    }

    public HazelcastInstance[] newInstances(Config config, int nodeCount) {
        final HazelcastInstance[] instances = new HazelcastInstance[nodeCount];
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

    public void shutdownAll() {
        if (mockNetwork) {
            nodeIndex.set(Integer.MAX_VALUE);
            registry.shutdown();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public void terminateAll() {
        if (mockNetwork) {
            nodeIndex.set(Integer.MAX_VALUE);
            registry.terminate();
        } else {
            HazelcastInstanceFactory.terminateAll();
        }
    }

    private static HazelcastInstance newHazelcastClient() {
        Class clazz = null;
        try {
            clazz = Class.forName(HAZELCAST_CLIENT);
            Class clientConfig = Class.forName(HAZELCAST_CLIENT_CONFIG);
            Method method = clazz.getMethod("newHazelcastClient", clientConfig);
            HazelcastInstance hazelcastInstance = (HazelcastInstance) method.invoke(null, clientConfig.newInstance());
            return hazelcastInstance;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static Address[] createAddresses(int count) {
        Address[] addresses = new Address[count];
        for (int i = 0; i < count; i++) {
            try {
                addresses[i] = new Address("127.0.0.1", PORTS.incrementAndGet());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return addresses;
    }

    private static Address[] createAddresses(String...addressArray) {
        checkElementsNotNull(addressArray);

        final int count = addressArray.length;
        Address[] addresses = new Address[count];
        for (int i = 0; i < count; i++) {
            try {
                addresses[i] = new Address(addressArray[i], PORTS.incrementAndGet());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return addresses;
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
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config.setProperty(GroupProperties.PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "120");
        config.setProperty(GroupProperties.PROP_PARTITION_BACKUP_SYNC_INTERVAL, "1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("TestHazelcastInstanceFactory{");
        sb.append("addresses=").append(Arrays.toString(addresses));
        sb.append('}');
        return sb.toString();
    }
}
