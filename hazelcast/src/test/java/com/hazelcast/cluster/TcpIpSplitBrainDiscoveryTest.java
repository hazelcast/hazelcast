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

package com.hazelcast.cluster;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.operation.UpdateTcpIpMemberListOperation;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class TcpIpSplitBrainDiscoveryTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "advancedNetwork:{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true}, {false},
        });
    }

    @Parameterized.Parameter
    public boolean advancedNetwork;

    List<HazelcastInstance> instances = new ArrayList<>();

    @After
    public void cleanup() {
        for (HazelcastInstance instance: instances) {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testSplitBrainRecoveryFromInitialSplit() {

        instances.add(Hazelcast.newHazelcastInstance(createConfig(advancedNetwork, 5801, 5901)));
        instances.add(Hazelcast.newHazelcastInstance(createConfig(advancedNetwork, 5901, 5801)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(0), instances.get(1)), 10);


        instances.add(Hazelcast.newHazelcastInstance(createConfig(advancedNetwork, 6001, 6101)));
        instances.add(Hazelcast.newHazelcastInstance(createConfig(advancedNetwork, 6101, 6001)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(2), instances.get(3)), 10);

        List<String> newMembers = Arrays.asList("localhost:" + 5801, "localhost:" + 5901, "localhost:" + 6001, "localhost:" + 6101);
        invokeOnStableClusterSerial(
                Accessors.getNodeEngineImpl(instances.get(3)),
                () -> new UpdateTcpIpMemberListOperation(newMembers), 5
        ).join();
        assertClusterSizeEventually(4, instances, 10);
    }

    protected static Config createConfig(boolean advancedNetwork, int port, int... otherMembersPorts) {
        Config config = new Config()
                .setProperty("hazelcast.merge.first.run.delay.seconds", "10")
                .setProperty("hazelcast.merge.next.run.delay.seconds", "10");
        JoinConfig joinConfig;
        if (!advancedNetwork) {
            NetworkConfig networkConfig = config.getNetworkConfig();
            networkConfig.setPortAutoIncrement(false).setPort(port);
            joinConfig = networkConfig.getJoin();
        } else {
            AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
            advancedNetworkConfig.setEnabled(true);
            ServerSocketEndpointConfig serverSocketEndpointConfig = new ServerSocketEndpointConfig();
            serverSocketEndpointConfig.setPort(port).setPortAutoIncrement(false);
            advancedNetworkConfig.setMemberEndpointConfig(serverSocketEndpointConfig);
            joinConfig = advancedNetworkConfig.getJoin();
        }
        joinConfig.getAutoDetectionConfig().setEnabled(false);
        joinConfig.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig().setEnabled(true).clear().addMember("localhost:" + port);
        for (int otherPort : otherMembersPorts) {
            tcpIpConfig.addMember("localhost:" + otherPort);
        }
        return config;
    }
}
