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

package com.hazelcast.cluster;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class TcpIpSplitBrainAutoDiscoveryTest extends HazelcastTestSupport {

    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-info.xml");

    @Parameters(name = "advancedNetwork:{0}")
    public static Object[] paramteters() {
        return new Object[] { false, true };
    }

    @Parameter
    public static boolean advancedNetwork;

    @After
    public void cleanup() {
        HazelcastInstanceFactory.terminateAll();
    }

    /**
     * Regression test (<a href="https://github.com/hazelcast/hazelcast/issues/18661">#18661</a>) for a case where a "supposed
     * master" doesn't know the other 2 member addresses. When this "supposed master" for some reason starts as the last one and
     * the previous 2 already created the cluster, they should be able to merge eventually.
     * <p>
     * This's equivalent to a case where there is a split brain in the cluster and the master from the smaller split doesn't
     * know the master address of the bigger split.
     */
    @Test
    public void regressionTest18661() throws IOException {
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(createConfig(5801, 5701));
        hz2.getMap("hz2").put("key", "value");
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(createConfig(5901, 5701, 5801));
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(createConfig(5701));
        hz1.getMap("hz1").put("key", "value");
        assertClusterSizeEventually(3, hz1, hz2, hz3);
        assertTrueEventually(() -> assertEquals("value", hz3.getMap("hz1").get("key")));
        assertTrueEventually(() -> assertEquals("value", hz1.getMap("hz2").get("key")));
    }

    /**
     * Regression test (<a href="https://github.com/hazelcast/hazelcast/issues/20331">#20331</a>) for a case where there
     * is an even number of members in the cluster and a split brain happens. When the two splits have the same number of
     * members, the merge (healing) is initiated from one side based on the lexicographic order of the master address. If the
     * "initiator" doesn't know the master address from the second split, but the other split knows the "initiator", the splits
     * should be able to merge.
     */
    @Test
    public void regressionTest20331() throws IOException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(createConfig(5701, 5801));
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(createConfig(5801));
        assertClusterSizeEventually(2, hz1, hz2);
    }

    @Test
    public void regressionTest20331Reverse() throws IOException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(createConfig(5801, 5701));
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(createConfig(5701));
        assertClusterSizeEventually(2, hz1, hz2);
    }

    protected static Config createConfig(int ownPort, int... otherPorts) {
        Config config = smallInstanceConfig().setProperty("hazelcast.merge.first.run.delay.seconds", "10")
                .setProperty("hazelcast.merge.next.run.delay.seconds", "10");
        JoinConfig joinConfig;
        if (!advancedNetwork) {
            NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPortAutoIncrement(false).setPort(ownPort);
            networkConfig.getRestApiConfig().setEnabled(true).enableAllGroups();
            joinConfig = networkConfig.getJoin();
        } else {
            AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
            advancedNetworkConfig.setEnabled(true);
            ServerSocketEndpointConfig serverSocketEndpointConfig = new ServerSocketEndpointConfig();
            serverSocketEndpointConfig.setPortAutoIncrement(false).setPort(ownPort);
            advancedNetworkConfig.setMemberEndpointConfig(serverSocketEndpointConfig);
            joinConfig = advancedNetworkConfig.getJoin();
        }
        joinConfig.getAutoDetectionConfig().setEnabled(false);
        joinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig().setEnabled(true).clear().addMember("127.0.0.1:" + ownPort);
        for (int otherPort : otherPorts) {
            tcpIpConfig.addMember("127.0.0.1:" + otherPort);
        }
        return config;
    }
}
