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

package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.discovery.multicast.MulticastDiscoveryStrategy;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.InputStream;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ClientToMemberDiscoveryTest extends HazelcastTestSupport {

    public final TestHazelcastFactory factory = new TestHazelcastFactory();

    Config serverConfig;
    ClientConfig clientConfig;

    @Parameter
    public Boolean safeSerialization;

    @Parameters(name = "{index}: safeSerialization: {0}")
    public static Object[] data() {
        return new Object[] { null, Boolean.FALSE, Boolean.TRUE };
    }

    @Before
    public void setup() {
        String serverXmlFileName = "hazelcast-multicast-plugin.xml";
        String clientXmlFileName = "hazelcast-client-multicast-plugin.xml";

        InputStream xmlResource = MulticastDiscoveryStrategy.class.getClassLoader().getResourceAsStream(serverXmlFileName);
        serverConfig = new XmlConfigBuilder(xmlResource).build();

        InputStream xmlClientResource = MulticastDiscoveryStrategy.class.getClassLoader()
                .getResourceAsStream(clientXmlFileName);
        clientConfig = new XmlClientConfigBuilder(xmlClientResource).build();
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void clientTest() {
        if (safeSerialization != null) {
            serverConfig.getNetworkConfig().getJoin().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                    .addProperty("safe-serialization", safeSerialization.toString());
            clientConfig.getNetworkConfig().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                    .addProperty("safe-serialization", safeSerialization.toString());
        }
        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void clientSerializationMismatchTest() {
        if (safeSerialization == null) {
            return;
        }
        serverConfig.getNetworkConfig().getJoin().getDiscoveryConfig().getDiscoveryStrategyConfigs().iterator().next()
                .addProperty("safe-serialization", safeSerialization.toString());
        clientConfig.getNetworkConfig().setConnectionTimeout(15000).getDiscoveryConfig().getDiscoveryStrategyConfigs()
                .iterator().next().addProperty("safe-serialization", Boolean.toString(!safeSerialization));
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3000);
        factory.newHazelcastInstance(serverConfig);
        factory.newHazelcastInstance(serverConfig);

        assertThrows(IllegalStateException.class, () -> factory.newHazelcastClient(clientConfig));
    }
}
