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

package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.properties.PropertyDefinition;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.instance.impl.TestUtil;
import com.hazelcast.internal.util.EmptyStatement;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.discovery.AbstractDiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.DiscoveryStrategy;
import com.hazelcast.spi.discovery.DiscoveryStrategyFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.starter.ReflectionUtils.setFieldValueReflectively;
import static org.junit.Assert.assertEquals;

/**
 * Regression test which checks the {@link NodeState} before the instance becomes {@link NodeState#ACTIVE}.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RestNodeStateTest {

    private static final int BASE_PORT = 5600;

    @Before
    @After
    public void cleanup() {
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
        int port = TestUtil.getAvailablePort(BASE_PORT);
        networkConfig.setPort(port).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.setRestApiConfig(restApiConfig);
        StrategyFactory discoveryStrategyFactory = new StrategyFactory();
        networkConfig.getJoin().getDiscoveryConfig()
                .addDiscoveryStrategyConfig(new DiscoveryStrategyConfig(discoveryStrategyFactory));
        HazelcastTestSupport.spawn(() -> Hazelcast.newHazelcastInstance(config));
        discoveryStrategyFactory.getNodeStartingLatch().await();
        HTTPCommunicator communicator = new HTTPCommunicator(port);
        assertEquals("\"STARTING\"", communicator.getClusterHealth("/node-state"));
        discoveryStrategyFactory.getTestDoneLatch().countDown();
    }

    @Test(timeout = 30000)
    public void testNodeStateAvailable_whenOtherMemberUnavailable() throws Exception {
        List<Integer> ports = TestUtil.getAvailablePorts(BASE_PORT, 2);
        Integer port1 = ports.get(0);
        Integer port2 = ports.get(1);
        Config config1 = config(port1);
        Config config2 = config(port2);
        HazelcastInstance member1 = Hazelcast.newHazelcastInstance(config1);
        Hazelcast.newHazelcastInstance(config2);
        // make member1 fail operations with HazelcastInstanceNotActiveException
        Node node1 = Accessors.getNode(member1);
        setFieldValueReflectively(node1, "state", NodeState.SHUT_DOWN);
        // query member2 about its node state
        // member shouldn't execute any cluster operation and respond immediately
        HTTPCommunicator communicator = new HTTPCommunicator(port2);
        assertEquals("\"ACTIVE\"", communicator.getClusterHealth("/node-state"));
    }

    private Config config(int port) {
        Config config = smallInstanceConfig();
        config.getNetworkConfig().getRestApiConfig().setEnabled(true).enableAllGroups();
        config.getNetworkConfig().setPort(port);
        return config;
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
