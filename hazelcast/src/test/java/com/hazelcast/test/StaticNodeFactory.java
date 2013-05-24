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

import com.hazelcast.client.*;
import com.hazelcast.client.MockSimpleClient;
import com.hazelcast.client.SimpleClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.*;
import com.hazelcast.nio.Address;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public final class StaticNodeFactory {

    public static final String HAZELCAST_TEST_USE_NETWORK = "hazelcast.test.use.network";
    public static final String HAZELCAST_TEST_USE_CLIENT = "hazelcast.test.use.client";

    public final static boolean MOCK_NETWORK = !Boolean.getBoolean(HAZELCAST_TEST_USE_NETWORK);
    public final static boolean USE_CLIENT = Boolean.getBoolean(HAZELCAST_TEST_USE_CLIENT);

    private final static String HAZELCAST_CLIENT = "com.hazelcast.client.HazelcastClient";
    private final static String HAZELCAST_CLIENT_CONFIG = "com.hazelcast.client.config.ClientConfig";
    private final static AtomicInteger ports = new AtomicInteger(5000);

    private final Address[] addresses;
    private final StaticNodeRegistry registry;
    private final AtomicInteger nodeIndex = new AtomicInteger();

    public StaticNodeFactory(int count) {
        if (MOCK_NETWORK) {
            addresses = createAddresses(count);
            registry = new StaticNodeRegistry(addresses);
        } else {
            addresses = null;
            registry = null;
        }
    }

    public HazelcastInstance newHazelcastInstance(Config config) {
        if (MOCK_NETWORK) {
            if (nodeIndex.get() >= addresses.length) {
                throw new IndexOutOfBoundsException("Max " + addresses.length + " instances can be created!");
            }
            config = init(config);
            NodeContext nodeContext = registry.createNodeContext(addresses[nodeIndex.getAndIncrement()]);
            return HazelcastInstanceFactory.newHazelcastInstance(config, null, nodeContext);
        }
        return USE_CLIENT ? newHazelcastClient() : HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    public HazelcastInstance[] newInstances(Config config) {
        final int count = addresses.length;
        final HazelcastInstance[] instances = new HazelcastInstance[count];
        for (int i = 0; i < count; i++) {
            instances[i] = newHazelcastInstance(config);
        }
        return instances;
    }

    public void shutdownAll() {
        if (MOCK_NETWORK) {
            nodeIndex.set(Integer.MAX_VALUE);
            registry.shutdown();
        } else {
            Hazelcast.shutdownAll();
        }
    }

    public static SimpleClient newClient(Address nodeAddress) throws IOException {
        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        for (HazelcastInstance hz : instances) {
            Node node = TestUtil.getNode(hz);
            MemberImpl m = (MemberImpl) hz.getCluster().getLocalMember();
            if (m.getAddress().equals(nodeAddress)) {
                if (MOCK_NETWORK) {
                    ClientEngineImpl engine = node.clientEngine;
                    return new MockSimpleClient(engine);
                } else {
                    return new SocketSimpleClient(node);
                }
            }
        }
        throw new IllegalArgumentException("Cannot connect to node: " + nodeAddress);
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
                addresses[i] = new Address("127.0.0.1", ports.incrementAndGet());
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        return addresses;
    }

    private static Config init(Config config) {
        if (config == null) {
            config = new XmlConfigBuilder().build();
        }
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config.setProperty(GroupProperties.PROP_GRACEFUL_SHUTDOWN_MAX_WAIT, "10");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        return config;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StaticNodeFactory{");
        sb.append("addresses=").append(Arrays.toString(addresses));
        sb.append('}');
        return sb.toString();
    }
}
