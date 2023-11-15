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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientTpcConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

import static com.hazelcast.client.impl.connection.tcp.TcpClientConnectionManager.getTargetTpcPorts;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TcpClientConnectionManagerTest extends ClientTestSupport {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setup() {
        factory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testGetTargetTpcPorts_whenConnectToAll() {
        ClientTpcConfig config = new ClientTpcConfig();
        List<Integer> tpcPorts = asList(1, 2, 3);

        // when larger than the number of tpc ports, return the full set.
        config.setConnectionCount(tpcPorts.size() + 1);
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));

        // when equal than the number of tpc ports, return the full set.
        config.setConnectionCount(tpcPorts.size());
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));

        // When 0, return the full set.
        config.setConnectionCount(0);
        assertEquals(tpcPorts, getTargetTpcPorts(tpcPorts, config));
    }

    @Test
    public void testGetTargetTpcPorts_whenConnectToSubset() {
        ClientTpcConfig config = new ClientTpcConfig();

        config.setConnectionCount(2);
        List<Integer> tpcPorts = asList(1, 2, 3);
        List<Integer> result = getTargetTpcPorts(tpcPorts, config);

        assertEquals(2, result.size());
        assertTrue(tpcPorts.containsAll(result));
    }

    @Test
    public void testIsUnisocketClient_whenTpcDisabledAndSmartRoutingDisabled() {
        verifyIsUnisocketClient(false, false);
    }

    @Test
    public void testIsUnisocketClient_whenTpcEnabledAndSmartRoutingDisabled() {
        verifyIsUnisocketClient(true, false);
    }

    @Test
    public void testIsUnisocketClient_whenTpcDisabledAndSmartRoutingEnabled() {
        verifyIsUnisocketClient(false, true);
    }

    @Test
    public void testIsUnisocketClient_whenTpcEnabledAndSmartRoutingEnabled() {
        verifyIsUnisocketClient(true, true);
    }

    private void verifyIsUnisocketClient(boolean tpcEnabled, boolean smartRouting) {
        ClientConfig config = new ClientConfig();
        config.getTpcConfig().setEnabled(tpcEnabled);
        config.getNetworkConfig().setSmartRouting(smartRouting);

        HazelcastInstance client = factory.newHazelcastClient(config);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);

        boolean isUnisocket = clientImpl.getConnectionManager().isUnisocketClient();
        // should be unisocket only when smart routing is false and TPC disabled
        assertEquals(!smartRouting && !tpcEnabled, isUnisocket);
    }

    @Test
    public void testSkipMemberListDuringReconnection() {
        HazelcastInstance instance = factory.newHazelcastInstance(smallInstanceConfigWithoutJetAndMetrics());

        Address address = instance.getCluster().getLocalMember().getAddress();
        String addressString = address.getHost() + ":" + address.getPort();
        ClientConfig config = new ClientConfig();
        config.setProperty(TcpClientConnectionManager.SKIP_MEMBER_LIST_DURING_RECONNECTION.getName(), "true");
        config.getNetworkConfig().setSmartRouting(false);
        config.getNetworkConfig().addAddress(addressString);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(3_000);

        // There are two members, and the unisocket client is connecting
        // to one of them. (the address of the `instance` defined above)
        HazelcastInstance client = factory.newHazelcastClient(config);

        assertEquals(2, client.getCluster().getMembers().size());
        instance.shutdown();

        // We shut down the `instance` the client is connected to but
        // there is still a member running. If the client was to try to
        // connect to members from the member list, it would succeed
        // and the assertion below would never be true.
        assertTrueEventually(() -> assertFalse(client.getLifecycleService().isRunning()));
    }
}
