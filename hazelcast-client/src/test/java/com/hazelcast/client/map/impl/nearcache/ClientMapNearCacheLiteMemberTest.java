/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.impl.nearcache.NearCacheLiteMemberTest;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.map.impl.nearcache.NearCacheLiteMemberTest.createNearCachedMapConfigWithMapStoreConfig;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapNearCacheLiteMemberTest {

    private String mapName;

    private TestHazelcastFactory factory;

    private HazelcastInstance client;
    private HazelcastInstance lite;

    @Before
    public void init() {
        mapName = randomMapName();

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(NearCacheLiteMemberTest.createConfig(mapName, false));

        client = factory.newHazelcastClient();
        lite = factory.newHazelcastInstance(NearCacheLiteMemberTest.createConfig(mapName, true));
    }

    @After
    public void destroy() {
        factory.terminateAll();
    }

    @Test
    public void testPut() {
        NearCacheLiteMemberTest.testPut(client, lite, mapName);
    }

    @Test
    public void testPutAll() {
        NearCacheLiteMemberTest.testPutAll(client, lite, mapName);
    }

    @Test
    public void testPutTransient() {
        NearCacheLiteMemberTest.testPutTransient(client, lite, mapName);
    }

    @Test
    public void testSet() {
        NearCacheLiteMemberTest.testSet(client, lite, mapName);
    }

    @Test
    public void testUpdate() {
        NearCacheLiteMemberTest.testUpdate(client, lite, mapName);
    }

    @Test
    public void testUpdateWithSet() {
        NearCacheLiteMemberTest.testUpdateWithSet(client, lite, mapName);
    }

    @Test
    public void testUpdateWithPutAll() {
        NearCacheLiteMemberTest.testUpdateWithPutAll(client, lite, mapName);
    }

    @Test
    public void testEvict() {
        NearCacheLiteMemberTest.testEvict(client, lite, mapName);
    }

    @Test
    public void testRemove() {
        NearCacheLiteMemberTest.testRemove(client, lite, mapName);
    }

    @Test
    public void testDelete() {
        NearCacheLiteMemberTest.testDelete(client, lite, mapName);
    }

    @Test
    public void testClear() {
        NearCacheLiteMemberTest.testClear(client, lite, mapName);
    }

    @Test
    public void testEvictAll() {
        NearCacheLiteMemberTest.testEvictAll(client, lite, mapName);
    }

    @Test
    public void testExecuteOnKey() {
        NearCacheLiteMemberTest.testExecuteOnKey(client, lite, mapName);
    }

    @Test
    public void testExecuteOnKeys() {
        NearCacheLiteMemberTest.testExecuteOnKeys(client, lite, mapName);
    }

    @Test
    public void testLoadAll() {
        initWithMapStore();

        NearCacheLiteMemberTest.testLoadAll(client, lite, mapName);
    }

    @Test
    public void testLoadAllWithKeySet() {
        initWithMapStore();

        NearCacheLiteMemberTest.testLoadAllWithKeySet(client, lite, mapName);
    }

    private void initWithMapStore() {
        factory.terminateAll();
        factory.newHazelcastInstance(createNearCachedMapConfigWithMapStoreConfig(mapName, false));

        client = factory.newHazelcastClient();
        lite = factory.newHazelcastInstance(createNearCachedMapConfigWithMapStoreConfig(mapName, true));
    }
}
