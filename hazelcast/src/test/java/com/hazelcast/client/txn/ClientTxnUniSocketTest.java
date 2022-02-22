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

package com.hazelcast.client.txn;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.connection.ClientConnectionManager;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.client.util.AbstractLoadBalancer;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientTxnUniSocketTest extends ClientTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testUniSocketClient_shouldNotOpenANewConnection() {
        hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setRedoOperation(true);
        config.getNetworkConfig().setSmartRouting(false);
        //try to force second member to connect when exist
        config.setLoadBalancer(new AbstractLoadBalancer() {
            @Override
            public Member next() {
                Member[] members = getMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                if (members.length == 2) {
                    return members[1];
                }
                return members[0];
            }

            @Override
            public Member nextDataMember() {
                Member[] members = getDataMembers();
                if (members == null || members.length == 0) {
                    return null;
                }
                if (members.length == 2) {
                    return members[1];
                }
                return members[0];
            }

            @Override
            public boolean canGetNextDataMember() {
                return true;
            }
        });
        final HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);
        hazelcastFactory.newHazelcastInstance();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, client.getCluster().getMembers().size());
            }
        });


        ClientConnectionManager connectionManager = getHazelcastClientInstanceImpl(client).getConnectionManager();

        TransactionContext context = client.newTransactionContext();

        context.beginTransaction();
        context.commitTransaction();

        assertEquals(1, connectionManager.getActiveConnections().size());
    }
}
