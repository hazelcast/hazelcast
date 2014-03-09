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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class NearCacheTest extends HazelcastTestSupport {

    @Test
    public void testBasicUsage() throws Exception {
        int n = 3;
        String mapName = "test";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        int count = 5000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (int i = 0; i < count; i++) {
            map.put(i, i * 2);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            int size = nearCache.size();
            Assert.assertTrue("NearCache Size: " + size, size > 0);
        }

        map.clear();
        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            Assert.assertEquals(0, nearCache.size());
        }

    }
    @Test
    public void testGetAll() throws Exception {
        final String mapName = "testGetAllWithNearCache";
        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig());
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i,i);
            keys.add(i);
        }
        long begin = System.currentTimeMillis();
        map.getAll(keys);
        long firstRead = System.currentTimeMillis() - begin;

        begin = System.currentTimeMillis();
        final Map<Integer,Integer> all = map.getAll(keys);
        long secondRead = System.currentTimeMillis() - begin;

        Assert.assertTrue(secondRead < firstRead);

        for (int i = 0; i < size; i++) {
            assertEquals(i,(int)all.get(i));

        }
    }


    private NearCache getNearCache(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = TestUtil.getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        return service.getNearCache(mapName);
    }

    // issue 1570
    @Test
    public void testNullValueNearCache() {
        int n = 2;
        String mapName = "testNullValueNearCache";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig());
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);

        int size = 100;

        for (int i = 0; i < size; i++) {
            assertNull(map.get("key" + i));
        }

        for (int i = 0; i < size; i++) {
            assertNull(map.get("key" + i));
        }

        assertTrue(map.getLocalMapStats().getGetOperationCount() < size*2);
    }
}
