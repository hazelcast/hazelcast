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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

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
    public void testOperationNotBlockingAfterClusterShutdown() throws InterruptedException {
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setConnectionAttemptLimit(Integer.MAX_VALUE);
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
