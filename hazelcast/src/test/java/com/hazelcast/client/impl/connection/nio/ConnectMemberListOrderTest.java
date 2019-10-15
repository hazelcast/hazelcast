/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.connection.nio;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClientDiscoveryService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.cluster.Address;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ConnectMemberListOrderTest extends ClientTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameterized.Parameters(name = "shuffleMemberList == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][]{{"false"}, {"true"}});
    }

    @Parameterized.Parameter
    public String shuffleMemberList;

    @After
    public void cleanup() {
        factory.shutdownAll();
    }

    @Test
    public void testPossibleMemberAddressesAfterDisconnection() throws Exception {
        HazelcastInstance instance = factory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), shuffleMemberList);
        final CountDownLatch connectedBack = new CountDownLatch(2);
        config.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
                    connectedBack.countDown();
                }
            }
        }));

        HazelcastInstance client = factory.newHazelcastClient(config);

        factory.newHazelcastInstance();
        factory.newHazelcastInstance();

        makeSureConnectedToServers(client, 3);
        Address lastConnectedMemberAddress = new Address((InetSocketAddress) instance.getLocalEndpoint().getSocketAddress());
        instance.shutdown();

        assertOpenEventually(connectedBack);

        Collection<Address> possibleMemberAddresses = getPossibleMemberAddresses(client);

        //make sure last known member list is used. otherwise it returns 2
        assertEquals(3, possibleMemberAddresses.size());

        //make sure previous owner is not first one to tried in next owner connection selection
        assertNotEquals("possibleMemberAddresses : " + possibleMemberAddresses + ", lastConnectedMemberAddress "
                + lastConnectedMemberAddress, possibleMemberAddresses.iterator().next(), lastConnectedMemberAddress);
    }

    private Collection<Address> getPossibleMemberAddresses(HazelcastInstance client) {
        HazelcastClientInstanceImpl instanceImpl = getHazelcastClientInstanceImpl(client);
        ClientDiscoveryService clientDiscoveryService = instanceImpl.getClientDiscoveryService();
        clientDiscoveryService.resetSearch();
        CandidateClusterContext clusterContext = clientDiscoveryService.current();
        ClusterConnectorServiceImpl clusterConnectorService = (ClusterConnectorServiceImpl) instanceImpl.getClusterConnectorService();
        return clusterConnectorService.getPossibleMemberAddresses(clusterContext.getAddressProvider());
    }

}
