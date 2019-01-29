/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spring;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.nio.ssl.TestKeyStoreUtil;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"clientNetworkConfig-applicationContext.xml"})
@Category(QuickTest.class)
public class TestClientNetworkConfig {

    @Resource(name = "client")
    private HazelcastClientProxy client;

    @BeforeClass
    @AfterClass
    public static void start() {
        String keyStoreFilePath = TestKeyStoreUtil.getKeyStoreFilePath();
        String trustStoreFilePath = TestKeyStoreUtil.getTrustStoreFilePath();

        System.setProperty("test.keyStore", keyStoreFilePath);
        System.setProperty("test.trustStore", trustStoreFilePath);

        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void smokeMember() {
        int memberCountInConfigurationXml = 2;
        ClientConfig config = client.getClientConfig();
        assertEquals(memberCountInConfigurationXml, config.getNetworkConfig().getAddresses().size());
    }

    @Test
    public void smokeSocketOptions() {
        int bufferSizeInConfigurationXml = 32;
        ClientConfig config = client.getClientConfig();
        assertEquals(bufferSizeInConfigurationXml, config.getNetworkConfig().getSocketOptions().getBufferSize());
    }

    @Test
    public void smokeSocketInterceptor() {
        ClientConfig config = client.getClientConfig();
        SocketInterceptorConfig socketInterceptorConfig = config.getNetworkConfig().getSocketInterceptorConfig();
        assertFalse(socketInterceptorConfig.isEnabled());
        assertEquals(DummySocketInterceptor.class.getName(), socketInterceptorConfig.getClassName());
    }

    @Test
    public void smokeSSLConfig() {
        ClientConfig config = client.getClientConfig();
        assertEquals("com.hazelcast.nio.ssl.BasicSSLContextFactory",
                config.getNetworkConfig().getSSLConfig().getFactoryClassName());
    }

    @Test
    public void smokeDiscoverySpiConfig() {
        DiscoveryConfig discoveryConfig = client.getClientConfig().getNetworkConfig().getDiscoveryConfig();
        assertNull(discoveryConfig.getDiscoveryServiceProvider());
        assertTrue(discoveryConfig.getNodeFilter() instanceof DummyNodeFilter);
        List<DiscoveryStrategyConfig> discoveryStrategyConfigs
                = (List<DiscoveryStrategyConfig>) discoveryConfig.getDiscoveryStrategyConfigs();
        assertEquals(4, discoveryStrategyConfigs.size());
        DiscoveryStrategyConfig discoveryStrategyConfig = discoveryStrategyConfigs.get(0);
        assertTrue(discoveryStrategyConfig.getDiscoveryStrategyFactory() instanceof DummyDiscoveryStrategyFactory);
        assertEquals(3, discoveryStrategyConfig.getProperties().size());
        assertEquals("foo", discoveryStrategyConfig.getProperties().get("key-string"));
        assertEquals("123", discoveryStrategyConfig.getProperties().get("key-int"));
        assertEquals("true", discoveryStrategyConfig.getProperties().get("key-boolean"));

        DiscoveryStrategyConfig discoveryStrategyConfig2 = discoveryStrategyConfigs.get(1);
        assertEquals(DummyDiscoveryStrategy.class.getName(), discoveryStrategyConfig2.getClassName());
        assertEquals(1, discoveryStrategyConfig2.getProperties().size());
        assertEquals("foo2", discoveryStrategyConfig2.getProperties().get("key-string"));

        DiscoveryStrategyConfig discoveryStrategyConfig3 = discoveryStrategyConfigs.get(2);
        assertEquals(DummyDiscoveryStrategy.class.getName(), discoveryStrategyConfig3.getClassName());

        DiscoveryStrategyConfig discoveryStrategyConfig4 = discoveryStrategyConfigs.get(3);
        assertTrue(discoveryStrategyConfig4.getDiscoveryStrategyFactory() instanceof DummyDiscoveryStrategyFactory);
    }

    @Test
    public void smokeOutboundPorts() {
        Collection<String> allowedPorts = client.getClientConfig().getNetworkConfig().getOutboundPortDefinitions();
        assertEquals(2, allowedPorts.size());
        assertTrue(allowedPorts.contains("34600"));
        assertTrue(allowedPorts.contains("34700-34710"));
    }
}
