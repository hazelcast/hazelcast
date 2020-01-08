/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.instance.ProtocolType.MEMBER;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueAllTheTime;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNode;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkStatsIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    private HazelcastInstance instance1;
    private HazelcastInstance instance2;

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionActive() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        enableMetrics(config);
        instance1 = newHazelcastInstance(config);
        instance2 = startSecondInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getBytesReceived(instance1, MEMBER) > 0);
                assertTrue(getBytesSent(instance1, MEMBER) > 0);
                assertTrue(getBytesReceived(instance2, MEMBER) > 0);
                assertTrue(getBytesSent(instance2, MEMBER) > 0);
            }
        });

        assertNonMemberNetworkStatsAreZero(instance1);
        assertNonMemberNetworkStatsAreZero(instance2);
    }

    @Test
    public void testStats_advancedNetworkEnabledAndConnectionClosed() {
        Config config = createCompleteMultiSocketConfig();
        configureTcpIpConfig(config);
        enableMetrics(config);
        instance1 = newHazelcastInstance(config);
        instance2 = startSecondInstance();
        assertClusterSizeEventually(2, instance1, instance2);

        instance2.shutdown();
        assertClusterSizeEventually(1, instance1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(getBytesReceived(instance1, MEMBER) > 0);
                assertTrue(getBytesSent(instance1, MEMBER) > 0);
            }
        });

        assertNonMemberNetworkStatsAreZero(instance1);
    }

    @Test
    public void testStats_advancedNetworkDisabled() {
        instance1 = newHazelcastInstance(getUnisocketConfig(MEMBER_PORT));
        instance2 = newHazelcastInstance(getUnisocketConfig(MEMBER_PORT + 1));
        assertClusterSizeEventually(2, instance1, instance2);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertAllNetworkStatsAreZero(instance1);
                assertAllNetworkStatsAreZero(instance2);
            }
        }, 30);
    }

    private Config getUnisocketConfig(int memberPort) {
        Config config1 = smallInstanceConfig();
        config1.getNetworkConfig().setPort(memberPort);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
               .addMember("127.0.0.1:" + MEMBER_PORT)
               .addMember("127.0.0.1:" + (MEMBER_PORT + 1));
        return config1;
    }

    private void assertAllNetworkStatsAreZero(HazelcastInstance instance) {
        assertEquals(0, getBytesReceived(instance, MEMBER));
        assertEquals(0, getBytesSent(instance, MEMBER));
        assertNonMemberNetworkStatsAreZero(instance);
    }

    private void assertNonMemberNetworkStatsAreZero(HazelcastInstance instance) {
        for (ProtocolType protocolType : ProtocolType.values()) {
            if (protocolType != MEMBER) {
                assertEquals(0, getBytesReceived(instance, protocolType));
                assertEquals(0, getBytesSent(instance, protocolType));
            }
        }
    }

    private long getBytesReceived(HazelcastInstance instance, ProtocolType protocolType) {
        AdvancedNetworkStats inboundStats = getNode(instance)
                .getNetworkingService()
                .getAggregateEndpointManager()
                .getInboundNetworkStats();
        return inboundStats != null ? inboundStats.getBytesTransceivedForProtocol(protocolType) : 0;
    }

    private long getBytesSent(HazelcastInstance instance, ProtocolType protocolType) {
        AdvancedNetworkStats outboundStats = getNode(instance)
                .getNetworkingService()
                .getAggregateEndpointManager()
                .getOutboundNetworkStats();
        return outboundStats != null ? outboundStats.getBytesTransceivedForProtocol(protocolType) : 0;
    }

    private void enableMetrics(Config config) {
        config.setProperty("hazelcast.diagnostics.enabled", "true");
        config.setProperty("hazelcast.diagnostics.metric.level", "Info");
        config.setProperty("hazelcast.diagnostics.metrics.period.seconds", "5");
    }

    private HazelcastInstance startSecondInstance() {
        Config config = prepareJoinConfigForSecondMember(MEMBER_PORT);
        enableMetrics(config);
        HazelcastInstance newHzInstance = newHazelcastInstance(config);
        int clusterSize = newHzInstance.getCluster().getMembers().size();
        assertEquals(2, clusterSize);
        return newHzInstance;
    }
}
