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

package com.hazelcast.client.test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HazelcastFactoryTest extends HazelcastTestSupport {

    private TestHazelcastFactory instanceFactory;

    @Before
    public void setUp() {
        instanceFactory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() {
        instanceFactory.terminateAll();
    }

    @Test
    public void testTestHazelcastInstanceFactory_smartClients() {
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        final HazelcastInstance client1 = instanceFactory.newHazelcastClient();
        final HazelcastInstance client2 = instanceFactory.newHazelcastClient();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                touchRandomNode(client1);
                touchRandomNode(client2);

                assertClusterSize(3, instance1, instance2, instance3);

                assertEquals(2, instance1.getClientService().getConnectedClients().size());
                assertEquals(2, instance2.getClientService().getConnectedClients().size());
                assertEquals(2, instance3.getClientService().getConnectedClients().size());
            }
        });
    }

    @Test
    public void testTestHazelcastInstanceFactory_dummyClients() {
        final HazelcastInstance instance1 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance2 = instanceFactory.newHazelcastInstance();
        final HazelcastInstance instance3 = instanceFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);
        final HazelcastInstance client1 = instanceFactory.newHazelcastClient(clientConfig);
        final HazelcastInstance client2 = instanceFactory.newHazelcastClient(clientConfig);

        assertClusterSizeEventually(3, instance1, instance2, instance3);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                touchRandomNode(client1);
                touchRandomNode(client2);

                int actual = instance1.getClientService().getConnectedClients().size()
                        + instance2.getClientService().getConnectedClients().size()
                        + instance3.getClientService().getConnectedClients().size();
                assertEquals(2, actual);
            }
        });

        assertClusterSizeEventually(3, client1, client2);
    }

    private static void touchRandomNode(HazelcastInstance hazelcastInstance) {
        hazelcastInstance.getMap(randomString()).get(randomString());
    }
}
