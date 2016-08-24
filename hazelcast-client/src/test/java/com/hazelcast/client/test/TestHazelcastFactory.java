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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Address;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHazelcastFactory extends TestHazelcastInstanceFactory {

    private static final AtomicInteger clientPorts = new AtomicInteger(40000);
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final List<HazelcastClientInstanceImpl> clients = new ArrayList<HazelcastClientInstanceImpl>(10);
    private final TestClientRegistry clientRegistry;

    public TestHazelcastFactory() {
        super(0);
        this.clientRegistry = new TestClientRegistry(registry);
    }

    public HazelcastInstance newHazelcastClient() {
        return newHazelcastClient(null);
    }

    public HazelcastInstance newHazelcastClient(ClientConfig config) {
        if (!mockNetwork) {
            return HazelcastClient.newHazelcastClient(config);
        }

        if (config == null) {
            config = new XmlClientConfigBuilder().build();
        }

        final ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            if (tccl == ClassLoader.getSystemClassLoader()) {
                Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            }
            ClientConnectionManagerFactory clientConnectionManagerFactory =
                    clientRegistry.createClientServiceFactory("127.0.0.1", clientPorts);
            AddressProvider testAddressProvider = createAddressProvider(config);
            HazelcastClientInstanceImpl client =
                    new HazelcastClientInstanceImpl(config, clientConnectionManagerFactory, testAddressProvider);
            client.start();
            clients.add(client);
            OutOfMemoryErrorDispatcher.registerClient(client);
            proxy = new HazelcastClientProxy(client);
        } finally {
            Thread.currentThread().setContextClassLoader(tccl);
        }
        return proxy;
    }

    private AddressProvider createAddressProvider(ClientConfig config) {

        List<String> userConfiguredAddresses = config.getNetworkConfig().getAddresses();
        if (!userConfiguredAddresses.contains("localhost")) {
            //Addresses are set explicitly. Dont add more addresses
            return null;
        }

        return new AddressProvider() {
            @Override
            public Collection<InetSocketAddress> loadAddresses() {
                Collection<InetSocketAddress> inetAddresses = new ArrayList<InetSocketAddress>();
                for (Address address : addressMap.values()) {
                    Collection<InetSocketAddress> addresses = AddressHelper.getPossibleSocketAddresses(address.getPort(),
                            address.getHost(), 3);
                    inetAddresses.addAll(addresses);
                }

                return inetAddresses;

            }
        };
    }

    @Override
    public void shutdownAll() {
        if (!mockNetwork) {
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.shutdown();
            }
        }
        super.shutdownAll();
    }

    public void shutdownAllMembers() {
        super.shutdownAll();
    }

    @Override
    public void terminateAll() {
        if (!mockNetwork) {
            //For client terminateAll and shutdownAll is same
            HazelcastClient.shutdownAll();
        } else {
            for (HazelcastClientInstanceImpl client : clients) {
                client.getLifecycleService().terminate();
            }
        }
        super.terminateAll();
    }
}
