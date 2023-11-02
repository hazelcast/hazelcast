/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.WAIT_SECONDS_BEFORE_JOIN;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;

/**
 * Check non-split-brain join for Discovery SPI
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DiscoverySPIJoinTest extends HazelcastTestSupport {
    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule2 = set(DISCOVERY_SPI_ENABLED.getName(), "true");
    @Rule
    public final OverridePropertyRule overridePropertyRule3 = set(WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
    private TestHazelcastInstanceFactory factory;

    public static class TestDiscoveryService implements DiscoveryService  {
        private static final List<DiscoveryNode> nodes = new ArrayList<>();

        public TestDiscoveryService(DiscoveryServiceSettings settings) {
        }

        @Override
        public void start() {
            try {
                nodes.add(new SimpleDiscoveryNode(new Address("127.0.0.1", 5701)));
                nodes.add(new SimpleDiscoveryNode(new Address("127.0.0.1", 5702)));
                nodes.add(new SimpleDiscoveryNode(new Address("127.0.0.1", 5703)));
            } catch (UnknownHostException ex) {
                throw new RuntimeException(ex);
            }
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return Collections.unmodifiableList(nodes);
        }

        @Override
        public void destroy() {
            nodes.clear();
        }

        @Override
        public Map<String, String> discoverLocalMetadata() {
            return Collections.unmodifiableMap(new HashMap<>());
        }
    }

    @Before
    public void beforeRun() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void afterRun() {
        factory.shutdownAll();
    }

    @Override
    protected Config getConfig() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin()
            .getDiscoveryConfig().setDiscoveryServiceProvider(TestDiscoveryService::new);
        return config;
    }

    @Test
    public void makeSureNoSplitBrain() {
        HazelcastInstance hz1 = factory.newHazelcastInstance(getConfig());
        HazelcastInstance hz2 = factory.newHazelcastInstance(getConfig());
        assertClusterStateEventually(ClusterState.ACTIVE, hz1, hz2);
        assertClusterSize(2, hz1, hz2);
    }
}
