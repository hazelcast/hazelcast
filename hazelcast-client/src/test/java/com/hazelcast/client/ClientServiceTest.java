/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.*;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientServiceTest extends HazelcastTestSupport {

    @Before
    @After
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testConnectedClients() {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();

        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();

        final ClientService clientService = instance.getClientService();
        final Collection<Client> connectedClients = clientService.getConnectedClients();
        assertEquals(2, connectedClients.size());

        final String uuid1 = client1.getLocalEndpoint().getUuid();
        final String uuid2 = client2.getLocalEndpoint().getUuid();
        for (Client connectedClient : connectedClients) {
            final String uuid = connectedClient.getUuid();
            assertTrue(uuid.equals(uuid1) || uuid.equals(uuid2));
        }

    }

    @Test
    public void testClientListener() throws InterruptedException {
        final HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        final ClientService clientService = instance.getClientService();
        final CountDownLatch latchAdd = new CountDownLatch(2);
        final CountDownLatch latchRemove = new CountDownLatch(2);
        final AtomicInteger totalAdd = new AtomicInteger(0);

        final ClientListener clientListener = new ClientListener() {
            @Override
            public void clientConnected(Client client) {
                totalAdd.incrementAndGet();
                latchAdd.countDown();
            }

            @Override
            public void clientDisconnected(Client client) {
                latchRemove.countDown();
            }
        };
        final String id = clientService.addClientListener(clientListener);

        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final HazelcastInstance client2 = HazelcastClient.newHazelcastClient();

        client1.getLifecycleService().shutdown();
        client2.getLifecycleService().shutdown();

        assertTrue(latchAdd.await(6, TimeUnit.SECONDS));
        assertTrue(latchRemove.await(6, TimeUnit.SECONDS));

        assertTrue(clientService.removeClientListener(id));

        assertFalse(clientService.removeClientListener("foo"));

        assertEquals(0, clientService.getConnectedClients().size());

        final HazelcastInstance client3 = HazelcastClient.newHazelcastClient();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(1, clientService.getConnectedClients().size());
            }
        }, 4);

        assertEquals(2, totalAdd.get());


    }

    @Test
    public void testClientListenerForBothNodes(){
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        final ClientListenerLatch clientListenerLatch = new ClientListenerLatch(2);

        final ClientService clientService1 = instance1.getClientService();
        clientService1.addClientListener(clientListenerLatch);
        final ClientService clientService2 = instance2.getClientService();
        clientService2.addClientListener(clientListenerLatch);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final String instance1Key = generateKeyOwnedBy(instance1);
        final String instance2Key = generateKeyOwnedBy(instance2);

        final IMap<Object, Object> map = client.getMap("map");
        map.put(instance1Key, 0);
        map.put(instance2Key, 0);

        assertClientConnected(clientService1, clientService2);
        assertOpenEventually(clientListenerLatch, 5);
    }

    private void assertClientConnected(ClientService... services){
        for (final ClientService service : services) {
            assertTrueEventually(new AssertTask() {
                @Override
                public void run() throws Exception {
                    assertEquals(1, service.getConnectedClients().size());
                }
            }, 5);
        }
    }

    public static class ClientListenerLatch extends CountDownLatch implements ClientListener {

        public ClientListenerLatch(int count) {
            super(count);
        }

        @Override
        public void clientConnected(Client client) {
            countDown();
        }

        @Override
        public void clientDisconnected(Client client) {
        }
    }


}