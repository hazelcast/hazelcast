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

package com.hazelcast.client.listeners;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListenerTests extends ClientTestSupport {

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void testSmartListenerRegister_whenNodeLeft() {
        int nodeCount = 5;
        for (int i = 0; i < nodeCount - 1; i++) {
            factory.newHazelcastInstance();
        }
        final HazelcastInstance node = factory.newHazelcastInstance();

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap("test");


        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                node.getLifecycleService().terminate();
            }
        }, 500, TimeUnit.MILLISECONDS);

        EntryAdapter listener = new EntryAdapter();

        LinkedList<String> registrationIds = new LinkedList<String>();
        while (client.getCluster().getMembers().size() == nodeCount) {
            registrationIds.add(map.addEntryListener(listener, false));
        }

        for (String registrationId : registrationIds) {
            assertTrue(map.removeEntryListener(registrationId));
        }
        executorService.shutdown();

    }

    @Test
    public void testSmartListenerRegister_whenNodeJoined() {
        int nodeCount = 5;
        for (int i = 0; i < nodeCount - 1; i++) {
            factory.newHazelcastInstance();
        }

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap("test");

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(new Runnable() {
            @Override
            public void run() {
                factory.newHazelcastInstance();
            }
        }, 500, TimeUnit.MILLISECONDS);

        EntryAdapter listener = new EntryAdapter();

        LinkedList<String> registrationIds = new LinkedList<String>();

        HazelcastClientInstanceImpl clientInstance = getHazelcastClientInstanceImpl(client);
        while (clientInstance.getConnectionManager().getActiveConnections().size() < nodeCount) {
            registrationIds.add(map.addEntryListener(listener, false));
        }

        for (String registrationId : registrationIds) {
            assertTrue(map.removeEntryListener(registrationId));
        }
        executorService.shutdown();

    }

    @Test
    public void testRemoveListenerOnClosedClient() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Object, Object> map = client.getMap("test");
        client.shutdown();
        assertTrue(map.removeEntryListener("test"));
    }
}
