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

package com.hazelcast.client;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class TestHazelcastFactoryTest extends HazelcastTestSupport {

    private HazelcastInstance server;
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Before
    public void setUp() throws Exception {
        server = factory.newHazelcastInstance();
    }

    @After
    public void tearDown() throws Exception {
        server.shutdown();
    }

    @Test
    public void no_leaking_client_after_shutdown() {
        HazelcastInstance client = factory.newHazelcastClient();
        HazelcastClientInstanceImpl clientInstanceImpl = ((HazelcastClientProxy) client).client;

        client.getLifecycleService().shutdown();

        assertNoLeakingClient(clientInstanceImpl);
    }

    @Test
    public void no_leaking_client_after_terminate() {
        HazelcastInstance client = factory.newHazelcastClient();
        HazelcastClientInstanceImpl clientInstanceImpl = ((HazelcastClientProxy) client).client;

        client.getLifecycleService().terminate();

        assertNoLeakingClient(clientInstanceImpl);
    }

    @Test
    public void no_leaking_client_after_shutdownAll() {
        HazelcastInstance client1 = factory.newHazelcastClient();
        HazelcastInstance client2 = factory.newHazelcastClient();
        HazelcastClientInstanceImpl clientInstanceImpl1 = ((HazelcastClientProxy) client1).client;
        HazelcastClientInstanceImpl clientInstanceImpl2 = ((HazelcastClientProxy) client2).client;

        factory.shutdownAll();

        assertNoLeakingClient(clientInstanceImpl1);
        assertNoLeakingClient(clientInstanceImpl2);
    }

    @Test
    public void no_leaking_client_after_terminateAll() {
        HazelcastInstance client1 = factory.newHazelcastClient();
        HazelcastInstance client2 = factory.newHazelcastClient();
        HazelcastClientInstanceImpl clientInstanceImpl1 = ((HazelcastClientProxy) client1).client;
        HazelcastClientInstanceImpl clientInstanceImpl2 = ((HazelcastClientProxy) client2).client;

        factory.terminateAll();

        assertNoLeakingClient(clientInstanceImpl1);
        assertNoLeakingClient(clientInstanceImpl2);
    }

    /**
     * Check if we removed all client object related
     * references from {@link OutOfMemoryErrorDispatcher}
     * and {@link HazelcastClient} internal client registry.
     */
    private static void assertNoLeakingClient(HazelcastClientInstanceImpl clientInstanceImpl) {
        String instanceName = clientInstanceImpl.getName();
        HazelcastInstance[] registeredClients = OutOfMemoryErrorDispatcher.getClientInstancesRef().get();
        for (HazelcastInstance registeredClient : registeredClients) {
            assertTrue("We expect no registered ref for the clientInstanceImpl",
                    clientInstanceImpl != registeredClient);
        }

        assertFalse(HazelcastClientUtil.hasRegisteredClientWithName(instanceName));
    }
}
