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

package com.hazelcast.instance;

import com.hazelcast.client.ClientEngineImpl;
import com.hazelcast.client.MockTestClient;
import com.hazelcast.client.SocketTestClient;
import com.hazelcast.client.TestClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.StaticNodeRegistry;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class StaticNodeFactory {

    public final static boolean MOCK_NETWORK = !Boolean.getBoolean("hazelcast.test.use.network");
    private final static boolean TEST_CLIENT = Boolean.getBoolean("hazelcast.test.client");
    private final static String HAZELCAST_CLIENT = "com.hazelcast.client.HazelcastClient";
    private final static String HAZELCAST_CLIENT_CONFIG = "com.hazelcast.client.config.ClientConfig";
    private final static AtomicInteger ports = new AtomicInteger(5000);

    private final Address[] addresses;
    private final StaticNodeRegistry registry;
    private int nodeIndex = 0;

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
            if (nodeIndex >= addresses.length) {
                throw new IndexOutOfBoundsException("Max " + addresses.length + " instances can be created!");
            }
            config = init(config);
            NodeContext nodeContext = registry.createNodeContext(addresses[nodeIndex++]);
            return HazelcastInstanceFactory.newHazelcastInstance(config, null, nodeContext);
        }
        return TEST_CLIENT ? newHazelcastClient() : HazelcastInstanceFactory.newHazelcastInstance(config);
    }

    public static TestClient newClient(Address nodeAddress) throws IOException {

        Set<HazelcastInstance> instances = Hazelcast.getAllHazelcastInstances();
        for (HazelcastInstance p : instances) {
            HazelcastInstanceImpl hz = ((HazelcastInstanceProxy) p).original;
            MemberImpl m = (MemberImpl) hz.getCluster().getLocalMember();
            if (m.getAddress().equals(nodeAddress)) {
                if (MOCK_NETWORK) {
                    ClientEngineImpl engine = hz.node.clientEngine;
                    return new MockTestClient(engine);
                } else {
                    return new SocketTestClient(hz);
                }
            }
        }
        return null;
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

    public static HazelcastInstance[] newInstances(Config config, int count) {
        final HazelcastInstance[] instances = new HazelcastInstance[count];
        if (MOCK_NETWORK) {
            config = init(config);
            Address[] addresses = createAddresses(count);
            StaticNodeRegistry staticNodeRegistry = new StaticNodeRegistry(addresses);
            for (int i = 0; i < count; i++) {
                NodeContext nodeContext = staticNodeRegistry.createNodeContext(addresses[i]);
                instances[i] = HazelcastInstanceFactory.newHazelcastInstance(config, null, nodeContext);
            }
        } else {
            for (int i = 0; i < count; i++) {
                instances[i] = TEST_CLIENT ? newHazelcastClient() : HazelcastInstanceFactory.newHazelcastInstance(config);
            }
        }
        return instances;
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
}
