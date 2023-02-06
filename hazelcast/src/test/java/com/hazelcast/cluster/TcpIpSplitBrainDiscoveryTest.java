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

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.RestApiConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.ascii.HTTPCommunicator;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.cluster.TcpIpSplitBrainDiscoveryTest.UpdateType.MEMBER_LIST_UPDATE;
import static com.hazelcast.cluster.TcpIpSplitBrainDiscoveryTest.UpdateType.WITH_OPERATION;
import static com.hazelcast.internal.util.InvocationUtil.invokeOnStableClusterSerial;

@RunWith(HazelcastParametrizedRunner.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(NightlyTest.class)
public class TcpIpSplitBrainDiscoveryTest extends HazelcastTestSupport {

    @Parameterized.Parameters(name = "advancedNetwork:{0}, updateType:{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {true, MEMBER_LIST_UPDATE},
                {true, WITH_OPERATION},
                {false, MEMBER_LIST_UPDATE},
                {false, WITH_OPERATION}
        });
    }

    @Parameterized.Parameter
    public static boolean advancedNetwork;

    @Parameterized.Parameter(1)
    public static UpdateType updateType;

    List<HazelcastInstance> instances = new ArrayList<>();

    @After
    public void cleanup() {
        for (HazelcastInstance instance: instances) {
            instance.getLifecycleService().terminate();
        }
    }

    @Test
    public void testSplitBrainRecoveryFromInitialSplit() throws IOException {
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(5801, 5901)));
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(5901, 5801)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(0), instances.get(1)));


        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(6001, 6101)));
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(6101, 6001)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(2), instances.get(3)));
        updateMemberList(3);
        assertClusterSizeEventually(4, instances);
    }

    @Test
    public void testAddressAreStoredForUnreachableNodes() throws IOException {
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(5801, 5901)));
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(5901, 5801)));
        assertClusterSizeEventually(2, Arrays.asList(instances.get(0), instances.get(1)));
        updateMemberList(1);

        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(6001, 6101)));
        instances.add(Hazelcast.newHazelcastInstance(createConfigWithRestEnabled(6101, 6001)));
        // first brain should find second brain eventually
        assertClusterSizeEventually(4, instances);
    }

    protected static Config createConfigWithRestEnabled(int port, int... otherMembersPorts) {
        Config config = new Config()
                .setProperty("hazelcast.merge.first.run.delay.seconds", "10")
                .setProperty("hazelcast.merge.next.run.delay.seconds", "10");
        JoinConfig joinConfig;
        boolean restEnabled = updateType != WITH_OPERATION;
        if (!advancedNetwork) {
            NetworkConfig networkConfig = config.getNetworkConfig();
            if (restEnabled) {
                RestApiConfig restApiConfig = new RestApiConfig().setEnabled(true).enableAllGroups();
                config.getNetworkConfig().setRestApiConfig(restApiConfig);
            }
            networkConfig.setPortAutoIncrement(false).setPort(port);
            joinConfig = networkConfig.getJoin();
        } else {
            AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
            advancedNetworkConfig.setEnabled(true);
            if (restEnabled) {
                advancedNetworkConfig.setRestEndpointConfig(new RestServerEndpointConfig().enableAllGroups());
            }
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

    protected void updateMemberList(int restNodeIndex) throws IOException {
        HTTPCommunicator communicator;
        switch (updateType) {
            case MEMBER_LIST_UPDATE:
                communicator = new HTTPCommunicator(instances.get(restNodeIndex));
                communicator.updateTcpIpMemberList(
                        instances.get(0).getConfig().getClusterName(),
                        "",
                        "localhost:5801, localhost:5901, localhost:6001, localhost:6101");
                break;
            case WITH_OPERATION:
                invokeOnStableClusterSerial(
                        Accessors.getNodeEngineImpl(instances.get(restNodeIndex)),
                        () -> new UpdateTcpIpMemberListOperation(
                                Arrays.asList("localhost:" + 5801, "localhost:" + 5901, "localhost:" + 6001, "localhost:" + 6101)),
                        5
                ).join();
                break;
        }
    }

    protected enum UpdateType {
        CONFIG_UPDATE, MEMBER_LIST_UPDATE, WITH_OPERATION
    }
}
