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
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapWideEvent;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @ali 24/10/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapIssueTest {

    @After
    public void reset(){
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testListenerRegistrations() throws Exception {
        final String mapName = "testListener";
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance client1 = HazelcastClient.newHazelcastClient();
        final IMap<Object, Object> map = client1.getMap(mapName);
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
            public void evictedAll(MapWideEvent event) {

            }
        }, true);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        instance1.getLifecycleService().terminate();
        instance1 = Hazelcast.newHazelcastInstance();
        final Field original = HazelcastInstanceProxy.class.getDeclaredField("original");
        original.setAccessible(true);
        final HazelcastInstanceImpl impl = (HazelcastInstanceImpl) original.get(instance1);
        final EventService eventService = impl.node.nodeEngine.getEventService();
        final Collection<EventRegistration> regs = eventService.getRegistrations(MapService.SERVICE_NAME, mapName);
        assertEquals("there should be only one registrations" , 1 , regs.size());
    }

    @Test
    public void testOperationNotBlockingAfterClusterShutdown() throws InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final IMap<String, String> m = client.getMap("m");


        m.put("elif", "Elif");
        m.put("ali", "Ali");
        m.put("alev", "Alev");


        instance1.getLifecycleService().terminate();
        instance2.getLifecycleService().terminate();

        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(){
            public void run() {
                try {
                    m.get("ali");
                } catch (Exception ignored) {
                    latch.countDown();
                }
            }
        }.start();

        assertTrue(latch.await(15, TimeUnit.SECONDS));

    }

    @Test
    public void testMapPagingEntries() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Map.Entry<Integer,Integer>> entries = map.entrySet(predicate);
        assertEquals(pageSize, entries.size());


    }

    @Test
    public void testMapPagingValues() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Collection<Integer> values = map.values(predicate);
        assertEquals(pageSize, values.size());


    }

    @Test
    public void testMapPagingKeySet() {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Integer, Integer> map = client.getMap("map");

        final int size = 50;
        final int pageSize = 5;
        for (int i = 0; i < size; i++) {
            map.put(size - i, i);
        }

        final PagingPredicate predicate = new PagingPredicate(pageSize);
        predicate.nextPage();

        final Set<Integer> values = map.keySet(predicate);
        assertEquals(pageSize, values.size());


    }



}
