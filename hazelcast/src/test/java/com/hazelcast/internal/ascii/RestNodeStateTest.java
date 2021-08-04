/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.ascii;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;

/**
 * Regression test which checks the {@link NodeState} before the instance becomes {@link NodeState#ACTIVE}.
 */
@RunWith(HazelcastParallelClassRunner.class)
public class RestNodeStateTest {

    @BeforeClass
    @AfterClass
    public static void cleanupClass() {
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * This test does a node-state REST call before the Node is switched to the {@link NodeState#ACTIVE}. A custom discovery
     * strategy is used to block the processing in a point when REST is already running, but the node is not yet active. During
     * the blocked discovery initialization the HTTP call is done.
     * <p>
     * See
     * <a href="https://github.com/hazelcast/hazelcast/issues/17773">https://github.com/hazelcast/hazelcast/issues/17773</a>.
     */
    @Test
    public void testStartingNodeState_regression() throws Exception {
        Config config = smallInstanceConfig().setProperty(ClusterProperty.DISCOVERY_SPI_ENABLED.getName(), "true");
        RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(5000).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.setRestApiConfig(restApiConfig);
        StrategyFactory discoveryStrategyFactory = new StrategyFactory();
        networkConfig.getJoin().getDiscoveryConfig()
                .addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(discoveryStrategyFactory));
        HazelcastTestSupport.spawn(() -> Hazelcast.newHazelcastInstance(config));
        discoveryStrategyFactory.getNodeStartingLatch().await();
        HTTPCommunicator communicator = new HTTPCommunicator(5000);
        assertEquals("\"STARTING\"", communicator.getClusterHealth("/node-state"));
        discoveryStrategyFactory.getTestDoneLatch().countDown();
    }

    public static class StrategyFactory implements DiscoveryStrategyFactory {

        final BlockingDiscoveryStrategy strategy = new BlockingDiscoveryStrategy();

        @Override
        public Class<? extends DiscoveryStrategy> getDiscoveryStrategyType() {
            return BlockingDiscoveryStrategy.class;
        }

        @Override
        public DiscoveryStrategy newDiscoveryStrategy(DiscoveryNode discoveryNode, ILogger logger,
                Map<String, Comparable> properties) {
            return strategy;
        }

        @Override
        public Collection<PropertyDefinition> getConfigurationProperties() {
            return Collections.emptySet();
        }

        public CountDownLatch getNodeStartingLatch() {
            return strategy.nodeStartingLatch;
        }

        public CountDownLatch getTestDoneLatch() {
            return strategy.testDoneLatch;
        }
    }

    public static class BlockingDiscoveryStrategy extends AbstractDiscoveryStrategy {

        final CountDownLatch nodeStartingLatch = new CountDownLatch(1);
        final CountDownLatch testDoneLatch = new CountDownLatch(1);

        @Override
        public void start() {
            try {
                nodeStartingLatch.countDown();
                testDoneLatch.await();
            } catch (InterruptedException e) {
                EmptyStatement.ignore(e);
            }
        }

        public BlockingDiscoveryStrategy() {
            super(null, Collections.emptyMap());
        }

        @Override
        public Iterable<DiscoveryNode> discoverNodes() {
            return Collections.emptyList();
        }
    }
}
