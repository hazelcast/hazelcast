/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.RoutingStrategy;
import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.clientside.ClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.DefaultClientConnectionManagerFactory;
import com.hazelcast.client.impl.clientside.DefaultClientExtension;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.config.RoutingMode;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.client.impl.spi.impl.ClientClusterServiceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientExtensionTest extends HazelcastTestSupport {
    private final ClientConnectionManagerFactory factory = new DefaultClientConnectionManagerFactory();
    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Test
    public void test_createServiceProxyFactory() {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertInstanceOf(ClientProxyFactory.class, clientExtension.createServiceProxyFactory(MapService.class));
    }

    @Test
    public void test_createServiceProxyFactory_whenUnknownServicePassed() {
        ClientExtension clientExtension = new DefaultClientExtension();
        assertThatThrownBy(() -> clientExtension.createServiceProxyFactory(TestService.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Proxy factory cannot be created. Unknown service");
    }

    @Test
    public void test_createClientClusterServiceWithMultiMemberRouting_throwsInvalidConfigurationException() throws IOException {
        String expectedMsg = "MULTI_MEMBER routing is an enterprise feature since 5.5. "
                + "You must use Hazelcast enterprise to enable this feature.";
        try (var classLoader = new URLClassLoader(new URL[0])) {
            ClientConfig clientConfig = new ClientConfig();
            // avoid loading anything but DefaultClientExtension
            clientConfig.setClassLoader(classLoader);
            clientConfig.getNetworkConfig().getClusterRoutingConfig().setRoutingMode(RoutingMode.MULTI_MEMBER)
                        .setRoutingStrategy(RoutingStrategy.PARTITION_GROUPS);
            var addressProvider = hazelcastFactory.createAddressProvider(clientConfig);

            assertThatThrownBy(() -> new HazelcastClientInstanceImpl(UUID.randomUUID().toString(),
                                                                     clientConfig, null,
                                                                     factory, addressProvider))
                    .isInstanceOf(InvalidConfigurationException.class)
                    .hasMessageContaining(expectedMsg);
        }
    }

    @Test
    public void test_createClientClusterService() throws IOException {
        ClientExtension clientExtension = new DefaultClientExtension();
        ClientConfig clientConfig = new ClientConfig();
        var addressProvider = hazelcastFactory.createAddressProvider(clientConfig);

        try (var classLoader = new URLClassLoader(new URL[0])) {
            // avoid loading anything but DefaultClientExtension
            clientConfig.setClassLoader(classLoader);
            var client = new HazelcastClientInstanceImpl(UUID.randomUUID().toString(),
                                                         clientConfig, null,
                                                         factory, addressProvider);
            assertInstanceOf(ClientClusterServiceImpl.class,
                             clientExtension.createClientClusterService(client));
        }
    }

    private static class TestService {

    }
}
