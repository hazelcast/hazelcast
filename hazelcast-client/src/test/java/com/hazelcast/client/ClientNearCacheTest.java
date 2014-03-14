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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheTest {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }
    @Test
    public void testNearCache() {
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(false);

        clientConfig.addNearCacheConfig("map*", new NearCacheConfig().setInMemoryFormat(InMemoryFormat.OBJECT));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap map = client.getMap("map1");

        for (int i = 0; i < 10 * 1000; i++) {
            map.put("key" + i, "value" + i);
        }

        long begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            map.get("key" + i);
        }

        long firstRead = System.currentTimeMillis() - begin;


        begin = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            map.get("key" + i);
        }
        long secondRead = System.currentTimeMillis() - begin;

        assertTrue(secondRead < firstRead);

    }

    @Test
    public void testGetAll() throws Exception {
        final String mapName = "testGetAllWithNearCache";
        ClientConfig config = new ClientConfig();
        config.addNearCacheConfig(mapName, new NearCacheConfig());
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        IMap<Integer, Integer> map = client.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i,i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        map.getAll(keys);
        NearCacheStats stats =   map.getLocalMapStats().getNearCacheStats();
        assertEquals(1000, stats.getHits());
    }

    @Test
    public void testGetAsync() throws Exception {
        final String mapName = "testGetAsyncWithNearCache";
        ClientConfig config = new ClientConfig();
        config.addNearCacheConfig(mapName, new NearCacheConfig());
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        IMap<Integer, Integer> map = client.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i,i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        for (int i = 0; i < size; i++) {
            Future<Integer> async = map.getAsync(i);
            async.get();
        }
        NearCacheStats stats =   map.getLocalMapStats().getNearCacheStats();
        assertEquals(1000, stats.getHits());

    }

    @Test
    public void testIssue2009() throws Exception {
        final String mapName = "testIssue2009";
        ClientConfig config = new ClientConfig();
        config.addNearCacheConfig(mapName, new NearCacheConfig());
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);
        IMap map = client.getMap(mapName);
        NearCacheStats stats =   map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

}
