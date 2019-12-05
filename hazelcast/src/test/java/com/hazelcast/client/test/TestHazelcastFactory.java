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

package com.hazelcast.client.test;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TestHazelcastFactory extends TestHazelcastInstanceFactory {

    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final List<HazelcastClientInstanceImpl> clients = Collections.synchronizedList(new ArrayList<>(10));
    private final TestClientRegistry clientRegistry = new TestClientRegistry(getRegistry());

    public TestHazelcastFactory(int initialPort, String... addresses) {
        super(initialPort, addresses);
    }

    public TestHazelcastFactory(int count) {
        super(count);
    }

    public TestHazelcastFactory() {
        this(0);
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

        Thread currentThread = Thread.currentThread();
        ClassLoader tccl = currentThread.getContextClassLoader();
        try {
            if (tccl == ClassLoader.getSystemClassLoader()) {
                currentThread.setContextClassLoader(HazelcastClient.class.getClassLoader());
            }
            HazelcastClientInstanceImpl client = new HazelcastClientInstanceImpl(config, null,
                    clientRegistry.createClientServiceFactory(), createAddressProvider(config));
            client.start();
            clients.add(client);
            OutOfMemoryErrorDispatcher.registerClient(client);
            return new HazelcastClientProxy(client);
        } finally {
            currentThread.setContextClassLoader(tccl);
        }
    }

    // used by MC tests
    public HazelcastInstance getHazelcastClientByName(String clientName) {
        return clients.stream()
                .filter(client -> client.getName().equals(clientName)).findFirst().orElse(null);
    }

    private AddressProvider createAddressProvider(ClientConfig config) {
        boolean discoveryEnabled = new HazelcastProperties(config.getProperties())
                .getBoolean(ClientProperty.DISCOVERY_SPI_ENABLED);

        List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                ClientAliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(config);

        List<String> userConfiguredAddresses = config.getNetworkConfig().getAddresses();

        boolean isAtLeastAProviderConfigured = discoveryEnabled || !aliasedDiscoveryConfigs.isEmpty()
                || !userConfiguredAddresses.isEmpty();

        if (isAtLeastAProviderConfigured) {
            // address providers or addresses are configured explicitly, don't add more addresses
            return null;
        }

        return new AddressProvider() {
            @Override
            public Addresses loadAddresses() {
                Addresses possibleAddresses = new Addresses();
                for (Address address : getKnownAddresses()) {
                    Addresses addresses = AddressHelper.getPossibleSocketAddresses(address.getPort(),
                            address.getHost(), 1);
                    possibleAddresses.addAll(addresses);
                }
                return possibleAddresses;
            }

            @Override
            public Address translate(Address address) {
                return address;
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
