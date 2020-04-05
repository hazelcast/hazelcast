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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.ClusterDiscoveryService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.properties.ClientProperty;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ConnectMemberListTest extends ClientTestSupport {

    @Parameterized.Parameters(name = "shuffleMemberList == {0}")
    public static Collection<Object[]> params() {
        return asList(new Object[][]{{"false"}, {"true"}});
    }

    @Parameterized.Parameter
    public String shuffleMemberList;

    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testPossibleMemberAddressesAfterDisconnection() {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperty.SHUFFLE_MEMBER_LIST.getName(), shuffleMemberList);
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        Collection<Address> possibleMemberAddresses = getPossibleMemberAddresses(client);
        //make sure last known member list is used. otherwise it returns 3
        assertEquals(4, possibleMemberAddresses.size());
    }

    private Collection<Address> getPossibleMemberAddresses(HazelcastInstance client) {
        HazelcastClientInstanceImpl instanceImpl = getHazelcastClientInstanceImpl(client);
        ClusterDiscoveryService clusterDiscoveryService = instanceImpl.getClusterDiscoveryService();
        CandidateClusterContext clusterContext = clusterDiscoveryService.current();
        TcpClientConnectionManager connectionManager = (TcpClientConnectionManager) instanceImpl.getConnectionManager();
        return connectionManager.getPossibleMemberAddresses(clusterContext.getAddressProvider());
    }

}
