/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class HazelcastFactoryTest extends HazelcastTestSupport {

    @Test
    public void testTestHazelcastInstanceFactory_smartClients() {
        TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        final HazelcastInstance client1 = instanceFactory.newHazelcastClient();
        final HazelcastInstance client2 = instanceFactory.newHazelcastClient();


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                touchRandomNode(client1);
                touchRandomNode(client2);

                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());

                assertEquals(2, instance1.getClientService().getConnectedClients().size());
                assertEquals(2, instance2.getClientService().getConnectedClients().size());
                assertEquals(2, instance3.getClientService().getConnectedClients().size());
            }
        });

        instanceFactory.terminateAll();

    }

    private void touchRandomNode(HazelcastInstance hazelcastInstance) {
        hazelcastInstance.getMap(randomString()).get(randomString());
    }

    @Test
    public void testTestHazelcastInstanceFactory_dummyClients() {
        TestHazelcastFactory instanceFactory = new TestHazelcastFactory();
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        final HazelcastInstance client1 = instanceFactory.newHazelcastClient(clientConfig);
        final HazelcastInstance client2 = instanceFactory.newHazelcastClient(clientConfig);


        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, instance1.getCluster().getMembers().size());
                assertEquals(3, instance2.getCluster().getMembers().size());
                assertEquals(3, instance3.getCluster().getMembers().size());
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                touchRandomNode(client1);
                touchRandomNode(client2);

                int actual = instance1.getClientService().getConnectedClients().size() +
                        instance2.getClientService().getConnectedClients().size() +
                        instance3.getClientService().getConnectedClients().size();
                assertEquals(2, actual);
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(3, client1.getCluster().getMembers().size());
                assertEquals(3, client1.getCluster().getMembers().size());
            }
        });

        instanceFactory.terminateAll();

    }
}
