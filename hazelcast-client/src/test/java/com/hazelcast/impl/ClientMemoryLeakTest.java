/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.client.ClientConfig;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.InstanceEvent;
import com.hazelcast.core.InstanceListener;
import com.hazelcast.util.AssertionUtil;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.util.AssertionUtil.assertTrueEventually;
import static com.hazelcast.util.AssertionUtil.sleepMillis;
import static com.hazelcast.util.AssertionUtil.sleepSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 15/06/15
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class ClientMemoryLeakTest {

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    /**
     * See issue #3308
     */
    @Test
    public void test_InstanceListener_deregisteration_when_client_is_shutdown() {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        HazelcastClient cl = HazelcastClient.newHazelcastClient(new ClientConfig());
        cl.addInstanceListener(new InstanceListener() {
            public void instanceCreated(InstanceEvent event) {
            }
            public void instanceDestroyed(InstanceEvent event) {
            }
        });
        cl.getLifecycleService().kill();

        final FactoryImpl factory = TestUtil.getNode(hz).factory;

        assertTrueEventually(new AssertionUtil.AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(factory.lsInstanceListeners.isEmpty());
            }
        }, 30);
    }

    /**
     * See issue #2829
     */
    @Test
    public void test_CallContext_leak_by_heartbeat_timer() throws InterruptedException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setConnectionTimeout(100);
        HazelcastClient.newHazelcastClient(clientConfig);

        ClientHandlerService clientHandlerService = TestUtil.getNode(hz).clientHandlerService;
        Collection<ClientEndpoint> endpoints = clientHandlerService.getClientEndpoints();

        assertEquals(1, endpoints.size());
        ClientEndpoint endpoint = endpoints.iterator().next();

        sleepSeconds(3);
        int max = endpoint.callContexts.size();
        assertTrue("current number of call-contexts: ", max < 5);

        for (int i = 0; i < 100; i++) {
            sleepMillis(10);
            assertEquals(max, endpoint.callContexts.size());
        }

    }
}
