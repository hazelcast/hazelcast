/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.cp.internal;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.CPGroupViewListenerService;
import com.hazelcast.client.impl.NoOpCPGroupViewListenerService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.internal.HazelcastRaftTestSupport;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CPGroupViewClientTest extends HazelcastRaftTestSupport {

    private TestHazelcastFactory factory;

    @Before
    public void setup() {
        factory = new TestHazelcastFactory();
    }

    @After
    public void after() {
        factory.terminateAll();
    }


    @Test
    public void test_UnlicensedClient_DoesNotReceiveLeaders() {
        Config config = new Config();
        config.getCPSubsystemConfig().setCPMemberCount(3).setGroupSize(3);

        HazelcastInstance[] instances = factory.newInstances(config, 3);
        assertClusterSizeEventually(3, instances);
        waitUntilCPDiscoveryCompleted(instances);


        // Assert member instances have the correct no-op listener impl
        for (HazelcastInstance instance : instances) {
            CPGroupViewListenerService service = getNode(instance).getClientEngine().getCPGroupViewListenerService();
            assertInstanceOf(NoOpCPGroupViewListenerService.class, service);
        }

        // Connect a client with direct to CP enabled & assert no CP leader information was sent
        HazelcastClientInstanceImpl client = createClient(true);

        // Assert the client is still able to utilize the CP subsystem
        assertClientCanModifyCP(client);
    }

    private HazelcastClientInstanceImpl createClient(boolean directToLeaderEnabled) {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setCPDirectToLeaderRoutingEnabled(directToLeaderEnabled);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        return ((HazelcastClientProxy) client).client;
    }

    private void assertClientCanModifyCP(HazelcastClientInstanceImpl client) {
        for (int k = 0; k < 10; k++) {
            client.getCPSubsystem().getAtomicLong("unrelated_long").getAndAdd(5);
        }
        assertEqualsEventually(() -> client.getCPSubsystem().getAtomicLong("unrelated_long").get(), 50L);
    }
}
