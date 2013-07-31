/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ILock;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @ali 7/3/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)
public class ClientIssueTest {

    @After
    @Before
    public void cleanup() throws Exception {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testClientPortConnection() {
        final Config config1 = new Config();
        config1.getGroupConfig().setName("foo");
        config1.getNetworkConfig().setPort(5701);
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(config1);

        final Config config2 = new Config();
        config2.getGroupConfig().setName("bar");
        config2.getNetworkConfig().setPort(5702);
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(config2);

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("bar");
        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap<Object, Object> map = client.getMap("map");
        assertNull(map.put("key", "value"));
        assertEquals(1, map.size());
    }

    /**
     * Test for issues #267 and #493
     */
    @Test
    public void testIssue493() throws Exception {

        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();


        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);

        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        final ILock lock = client.getLock("lock");

        for (int k = 0; k < 10; k++) {
            lock.lock();
            try {
                Thread.sleep(100);
            } finally {
                lock.unlock();
            }
        }

        lock.lock();
        hz1.getLifecycleService().shutdown();
        lock.unlock();
    }

    @Test
    public void testOperationRedo() throws Exception {
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setRedoOperation(true);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final Thread thread = new Thread() {
            public void run() {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                hz1.getLifecycleService().terminate();
            }
        };

        final IMap map = client.getMap("m");
        thread.start();
        int expected = 1000;
        for (int i = 0; i < expected; i++) {
            map.put(i, "item" + i);
        }
        thread.join();
        assertEquals(expected, map.size());
    }

    @Test
    public void testNearCache(){
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance();
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance();

        final ClientConfig clientConfig = new ClientConfig();
        clientConfig.setSmart(false);

        clientConfig.addNearCacheConfig("map*", new NearCacheConfig().setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT));

        final HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);

        final IMap map = client.getMap("map1");

        for (int i=0; i<10*1000; i++){
            map.put("key"+i, "value"+i);
        }

        long begin = System.currentTimeMillis();
        for (int i=0; i<1000; i++){
            map.get("key"+i);
        }

        long firstRead = System.currentTimeMillis() - begin;


        begin = System.currentTimeMillis();
        for (int i=0; i<1000; i++){
            map.get("key"+i);
        }
        long secondRead = System.currentTimeMillis() - begin;

        assertTrue(secondRead < firstRead);

    }
}
