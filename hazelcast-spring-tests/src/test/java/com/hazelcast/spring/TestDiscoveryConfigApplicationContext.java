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

package com.hazelcast.spring;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.WanBatchPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;

import javax.annotation.Resource;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(CustomSpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"discoveryConfig-applicationContext-hazelcast.xml"})
@Category(QuickTest.class)
@SuppressWarnings("unused")
public class TestDiscoveryConfigApplicationContext {

    private Config config;

    @Resource(name = "instance")
    private HazelcastInstance instance;

    @BeforeClass
    @AfterClass
    public static void start() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Before
    public void before() {
        config = instance.getConfig();
    }

    @Test
    public void testNetworkDiscoveryConfig() {
        NetworkConfig networkConfig = config.getNetworkConfig();

        assertDiscoveryConfig(networkConfig.getJoin().getDiscoveryConfig());
    }

    @Test
    public void testWanDiscoveryConfig() {
        WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan");
        WanBatchPublisherConfig publisherConfig = wcfg.getBatchPublisherConfigs().get(0);

        assertDiscoveryConfig(publisherConfig.getDiscoveryConfig());
    }

    private void assertDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        assertTrue(discoveryConfig.getDiscoveryServiceProvider() instanceof DummyDiscoveryServiceProvider);
        assertTrue(discoveryConfig.getNodeFilter() instanceof DummyNodeFilter);
        List<DiscoveryStrategyConfig> discoveryStrategyConfigs
                = (List<DiscoveryStrategyConfig>) discoveryConfig.getDiscoveryStrategyConfigs();
        assertEquals(1, discoveryStrategyConfigs.size());
        DiscoveryStrategyConfig discoveryStrategyConfig = discoveryStrategyConfigs.get(0);
        assertTrue(discoveryStrategyConfig.getDiscoveryStrategyFactory() instanceof DummyDiscoveryStrategyFactory);
        assertEquals(3, discoveryStrategyConfig.getProperties().size());
        assertEquals("foo", discoveryStrategyConfig.getProperties().get("key-string"));
        assertEquals("123", discoveryStrategyConfig.getProperties().get("key-int"));
        assertEquals("true", discoveryStrategyConfig.getProperties().get("key-boolean"));
    }
}
