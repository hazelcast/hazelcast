/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.HazelcastClientUtil;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.config.impl.ClientAliasedDiscoveryConfigUtils;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.Addresses;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.util.AddressHelper;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.TestHazelcastInstanceFactory;

import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.HazelcastClientUtil.getInstanceName;

public class TestHazelcastFactory extends TestHazelcastInstanceFactory {

    public static final String TEST_JVM_PREFIX = "test-jvm-";
    private static final String jvmName = ManagementFactory.getRuntimeMXBean().getName();
    private final boolean mockNetwork = TestEnvironment.isMockNetwork();
    private final ConcurrentMap<String, HazelcastClientInstanceImpl> clients = new ConcurrentHashMap<>(10);
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
        return newHazelcastClient(config, null);
    }

    public HazelcastInstance newHazelcastClient(ClientConfig config, String sourceIp) {
        if (!mockNetwork) {
            HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
            registerJvmNameAndPidMetric(((HazelcastClientProxy) client).client);
            return client;
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
            String instanceName = getInstanceName(config);
            HazelcastClientInstanceImpl client = new HazelcastClientInstanceImpl(instanceName, config,
                    null, clientRegistry.createClientServiceFactory(sourceIp), createAddressProvider(config));
            registerJvmNameAndPidMetric(client);
            client.start();
            if (clients.putIfAbsent(client.getName(), client) != null) {
                throw new InvalidConfigurationException("HazelcastClientInstance with name '" + client.getName()
                        + "' already exists!");
            }

            HazelcastInstanceFactory.InstanceFuture future = new HazelcastInstanceFactory.InstanceFuture<>();
            HazelcastClientUtil.registerProxyFuture(instanceName, future);

            OutOfMemoryErrorDispatcher.registerClient(client);
            HazelcastClientProxy proxy = new HazelcastClientProxy(client);
            future.set(proxy);
            return proxy;
        } finally {
            currentThread.setContextClassLoader(tccl);
        }
    }

    private void registerJvmNameAndPidMetric(HazelcastClientInstanceImpl client) {
        int pid = Integer.parseInt(jvmName.substring(0, jvmName.indexOf("@")));
        MetricsRegistryImpl metricsRegistry = client.getMetricsRegistry();
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context
                        .collect(descriptor.withPrefix(TEST_JVM_PREFIX + jvmName).withMetric("pid"), pid));
    }

    // used by MC tests
    public HazelcastInstance getHazelcastClientByName(String clientName) {
        return clients.get(clientName);
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

            @Override
            public Address translate(Member member) {
                return member.getAddress();
            }
        };
    }

    public void shutdownAllMembers() {
        super.shutdownAll();
    }

    @Override
    public void shutdownAll() {
        if (mockNetwork) {
            for (HazelcastClientInstanceImpl client : clients.values()) {
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
            for (HazelcastClientInstanceImpl client : clients.values()) {
                client.getLifecycleService().terminate();
            }
        } else {
            // for client terminateAll() and shutdownAll() is the same
            HazelcastClient.shutdownAll();
        }
        super.terminateAll();
    }
}
