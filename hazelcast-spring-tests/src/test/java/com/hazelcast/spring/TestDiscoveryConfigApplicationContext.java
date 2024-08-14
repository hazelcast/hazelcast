/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;


@ExtendWith({SpringExtension.class, CustomSpringExtension.class})
@ContextConfiguration(locations = {"discoveryConfig-applicationContext-hazelcast.xml"})
@SuppressWarnings("unused")
class TestDiscoveryConfigApplicationContext {

    private Config config;

    @Autowired
    private HazelcastInstance instance;

    @BeforeAll
    @AfterAll
    public static void start() {
        HazelcastInstanceFactory.terminateAll();
    }

    @BeforeEach
    public void before() {
        config = instance.getConfig();
    }

    @Test
    void testNetworkDiscoveryConfig() {
        NetworkConfig networkConfig = config.getNetworkConfig();

        assertDiscoveryConfig(networkConfig.getJoin().getDiscoveryConfig());
    }

    @Test
    void testWanDiscoveryConfig() {
        WanReplicationConfig wcfg = config.getWanReplicationConfig("testWan");
        WanBatchPublisherConfig publisherConfig = wcfg.getBatchPublisherConfigs().get(0);

        assertDiscoveryConfig(publisherConfig.getDiscoveryConfig());
    }

    private void assertDiscoveryConfig(DiscoveryConfig discoveryConfig) {
        assertInstanceOf(DummyDiscoveryServiceProvider.class, discoveryConfig.getDiscoveryServiceProvider());
        assertInstanceOf(DummyNodeFilter.class, discoveryConfig.getNodeFilter());
        List<DiscoveryStrategyConfig> discoveryStrategyConfigs
                = (List<DiscoveryStrategyConfig>) discoveryConfig.getDiscoveryStrategyConfigs();
        assertEquals(1, discoveryStrategyConfigs.size());
        DiscoveryStrategyConfig discoveryStrategyConfig = discoveryStrategyConfigs.get(0);
        assertInstanceOf(DummyDiscoveryStrategyFactory.class, discoveryStrategyConfig.getDiscoveryStrategyFactory());
        assertEquals(3, discoveryStrategyConfig.getProperties().size());
        assertEquals("foo", discoveryStrategyConfig.getProperties().get("key-string"));
        assertEquals("123", discoveryStrategyConfig.getProperties().get("key-int"));
        assertEquals("true", discoveryStrategyConfig.getProperties().get("key-boolean"));
    }
}
