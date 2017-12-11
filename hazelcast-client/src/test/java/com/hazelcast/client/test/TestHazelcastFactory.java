/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.config.ClientAwsConfig;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.connection.AddressProvider;
import com.hazelcast.client.impl.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.spi.properties.ClientProperty;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestHazelcastFactory extends TestHazelcastInstanceFactory {

    private static final AtomicInteger CLIENT_PORTS = new AtomicInteger(40000);

    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final List<HazelcastClientInstanceImpl> clients = new ArrayList<HazelcastClientInstanceImpl>(10);
    private final TestClientRegistry clientRegistry;

    public TestHazelcastFactory() {
        this(0);
    }

    public TestHazelcastFactory(int count) {
        super(count);
        this.clientRegistry = new TestClientRegistry(getRegistry());
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

        ClassLoader tccl = Thread.currentThread().getContextClassLoader();
        HazelcastClientProxy proxy;
        try {
            if (tccl == ClassLoader.getSystemClassLoader()) {
                Thread.currentThread().setContextClassLoader(HazelcastClient.class.getClassLoader());
            }
            ClientConnectionManagerFactory clientConnectionManagerFactory =
                    clientRegistry.createClientServiceFactory("127.0.0.1", CLIENT_PORTS);
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
        boolean discoveryEnabled = new HazelcastProperties(config.getProperties())
                .getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED);

        ClientAwsConfig awsConfig = config.getNetworkConfig().getAwsConfig();

        List<String> userConfiguredAddresses = config.getNetworkConfig().getAddresses();

        boolean isAtLeastAProviderConfigured =
                discoveryEnabled || (awsConfig != null ? awsConfig.isEnabled() : false) || !userConfiguredAddresses.isEmpty();

        if (isAtLeastAProviderConfigured) {
            // address providers or addresses are configured explicitly, don't add more addresses
            return null;
        }

        return new AddressProvider() {
            @Override
            public Collection<Address> loadAddresses() {
                Collection<Address> possibleAddresses = new ArrayList<Address>();
                for (Address address : getKnownAddresses()) {
                    Collection<Address> addresses = AddressHelper.getPossibleSocketAddresses(address.getPort(),
                            address.getHost(), 1);
                    possibleAddresses.addAll(addresses);
                }
                return possibleAddresses;
            }
        };
    }

    public void shutdownAllMembers() {
        super.shutdownAll();
    }

    @Override
    public void shutdownAll() {
        if (mockNetwork) {
            for (HazelcastClientInstanceImpl client : clients) {
                client.shutdown();
            }
        } else {
            // for client terminateAll() and shutdownAll() is the same
            HazelcastClient.shutdownAll();
        }
        super.shutdownAll();
    }

    @Override
    public void terminateAll() {
        if (mockNetwork) {
            for (HazelcastClientInstanceImpl client : clients) {
                client.getLifecycleService().terminate();
            }
        } else {
            // for client terminateAll() and shutdownAll() is the same
            HazelcastClient.shutdownAll();
        }
        super.terminateAll();
    }
}
