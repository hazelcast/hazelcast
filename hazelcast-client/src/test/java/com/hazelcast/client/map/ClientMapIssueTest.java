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

package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.map.MapService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * @ali 24/10/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapIssueTest extends HazelcastTestSupport {

    private HazelcastInstance server1;
    private HazelcastInstance server2;

    private HazelcastInstance client1;

    @Before
    public void setup() {
        server1 = Hazelcast.newHazelcastInstance();
        server2 = Hazelcast.newHazelcastInstance();
        client1 = HazelcastClient.newHazelcastClient();
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testListenerRegistrations() throws Exception {
        String mapName = "testListener";

        IMap<Object, Object> map = client1.getMap(mapName);
        map.addEntryListener(new EntryListener<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        }, true);

        server1.getLifecycleService().terminate();
        server1 = Hazelcast.newHazelcastInstance();

        Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);
        HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(server1);
        EventService eventService = impl.node.nodeEngine.getEventService();
        Collection<EventRegistration> regs = eventService.getRegistrations(MapService.SERVICE_NAME, mapName);

        assertEquals("there should be only one registrations", 1, regs.size());
    }

    @Test
    public void testOperationNotBlockingAfterClusterShutdown() throws InterruptedException {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<String, String> map = client.getMap("m");
        map.put("elif", "Elif");
        map.put("ali", "Ali");
        map.put("alev", "Alev");

        server1.getLifecycleService().terminate();
        server2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread() {
            public void run() {
                try {
                    map.get("ali");
                } catch (Exception ignored) {
                    latch.countDown();
                }
            }
        }.start();

        assertOpenEventually(latch);
    }

    @Test
    public void testMapPagingEntries() {
        IMap<Integer, Integer> map = client1.getMap("map");

        int size = 50;
        int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        Set<Map.Entry<Integer, Integer>> entries = map.entrySet(predicate);
        assertEquals(pageSize, entries.size());
    }

    @Test
    public void testMapPagingValues() {
        IMap<Integer, Integer> map = client1.getMap("map");

        int size = 50;
        int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());
    }

    @Test
    public void testMapPagingKeySet() {
        IMap<Integer, Integer> map = client1.getMap("map");

        int size = 50;
        int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }

        PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        Set<Integer> values = map.keySet(predicate);
        assertEquals(pageSize, values.size());
    }
}
