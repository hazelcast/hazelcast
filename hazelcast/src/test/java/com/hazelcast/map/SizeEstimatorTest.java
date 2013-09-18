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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizeConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class SizeEstimatorTest extends HazelcastTestSupport {

    @Test
    public void testIdleState() throws InterruptedException {
        final String MAP_NAME = "default";

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h = factory.newHazelcastInstance(null);

        final IMap<String, String> map = h.getMap(MAP_NAME);

        Assert.assertTrue(map.getLocalMapStats().getHeapCost() == 0);
    }

    @Test
    public void testPuts() throws InterruptedException{
        final String MAP_NAME = "default";

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance h = factory.newHazelcastInstance(null);

        final IMap<Long, Long> map = h.getMap(MAP_NAME);

        map.put(10L,10L);


        Assert.assertTrue(map.getLocalMapStats().getHeapCost() == 152);

    }

    @Test
    public void testPutRemove() throws InterruptedException {
        final String MAP_NAME = "default";

        final Config config = new Config();
        config.getMapConfig(MAP_NAME).setBackupCount(1).setInMemoryFormat(MapConfig.InMemoryFormat.BINARY);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h[] = factory.newInstances(config);
        warmUpPartitions(h);

        final IMap<Long, Long> map = h[0].getMap(MAP_NAME);
        final IMap<Long, Long> backupMap = h[1].getMap(MAP_NAME);

        map.put(10L, 10L);

        Thread.sleep(3000);

        long h1MapCost = map.getLocalMapStats().getHeapCost();

        long h2MapCost = backupMap.getLocalMapStats().getHeapCost();

        // one map is backup. so backup & main map cost must be same.
        Assert.assertEquals(h1MapCost, h2MapCost);

        Thread.sleep(1000);

        map.remove( 10L );

        Thread.sleep(1000);

        h1MapCost = map.getLocalMapStats().getHeapCost();

        h2MapCost = backupMap.getLocalMapStats().getHeapCost();

        Assert.assertEquals(0, h1MapCost);
        Assert.assertEquals(0, h2MapCost);
    }

    @Test
    public void testEvictionPolicy() throws InterruptedException {
        final String MAP_NAME = "default";

        final Config config = new Config();
        final MaxSizeConfig mapMaxSizeConfig = new MaxSizeConfig();
        mapMaxSizeConfig.setSize(1).setMaxSizePolicy(MaxSizeConfig.MaxSizePolicy.USED_HEAP_SIZE);
        config.getMapConfig(MAP_NAME)
                .setEvictionPolicy(MapConfig.EvictionPolicy.LFU).
                setMaxSizeConfig(mapMaxSizeConfig).setEvictionPercentage(100);

        final int n = 1;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance h[] = factory.newInstances(config);
        warmUpPartitions(h);

        final IMap<Long, Integer> map = h[0].getMap(MAP_NAME);

        Long key = 0L;
        for (int i = 0; i < 1; i++) {
            map.put(++key, 1);
        }
        long t = map.getLocalMapStats().getHeapCost();
        /** key 24 bytes  {@link com.hazelcast.nio.serialization.Data} totalSize method */
        /** value 112 bytes {@link com.hazelcast.map.record.DataRecord}*/
        Assert.assertTrue(map.getLocalMapStats().getHeapCost() == 148);

        Thread.sleep(1000);

        map.clear();

        Thread.sleep(1000);

        Assert.assertTrue(map.getLocalMapStats().getHeapCost() == 0);
    }

    @Test
    public void testNearCache() throws InterruptedException {
        final String NO_NEAR_CAHED_MAP = "testIssue833";
        final String NEAR_CACHED_MAP = "testNearCache";

        final Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(MapConfig.InMemoryFormat.BINARY);
        config.getMapConfig(NEAR_CACHED_MAP).setNearCacheConfig(nearCacheConfig).setBackupCount(0);
        config.getMapConfig(NO_NEAR_CAHED_MAP).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance h[] = factory.newInstances(config);
        warmUpPartitions(h);

        final IMap<String, String> noNearCached = h[0].getMap(NO_NEAR_CAHED_MAP);
        noNearCached.put("key", "value");
        noNearCached.put("key1", "value1");
        noNearCached.put("key2", "value2");
        noNearCached.put("key3", "value3");

        final IMap<String, String> nearCachedMap = h[0].getMap(NEAR_CACHED_MAP);
        nearCachedMap.put("key", "value");
        nearCachedMap.put("key1", "value1");
        nearCachedMap.put("key2", "value2");
        nearCachedMap.put("key3", "value3");

        for (int i = 0; i < 100; i++) {
            nearCachedMap.get("key");
            nearCachedMap.get("key1");
            nearCachedMap.get("key2");
            nearCachedMap.get("key3");

        }

        Assert.assertTrue(nearCachedMap.getLocalMapStats().getHeapCost() > noNearCached.getLocalMapStats().getHeapCost());
    }

    @Test
    public void testInMemoryFormats() throws InterruptedException {
        final String BINARY_MAP = "testBinaryFormat";
        final String OBJECT_MAP = "testObjectFormat";
        final String CACHED_MAP = "testCachedFormat";
        final Config config = new Config();
        config.getMapConfig(BINARY_MAP).
                setInMemoryFormat(MapConfig.InMemoryFormat.BINARY).setBackupCount(0);
        config.getMapConfig(OBJECT_MAP).
                setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT).setBackupCount(0);
        config.getMapConfig(CACHED_MAP).
                setInMemoryFormat(MapConfig.InMemoryFormat.CACHED).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance[] h = factory.newInstances(config);
        warmUpPartitions(h);

        // populate map.
        final IMap<String, String> binaryMap = h[0].getMap(BINARY_MAP);
        binaryMap.put("key", "value");
        binaryMap.put("key1", "value1");
        binaryMap.put("key2", "value2");
        binaryMap.put("key3", "value3");

        final IMap<String, String> objectMap = h[0].getMap(OBJECT_MAP);
        objectMap.put("key", "value");
        objectMap.put("key1", "value1");
        objectMap.put("key2", "value2");
        objectMap.put("key3", "value3");

        final IMap<String, String> cachedMap = h[0].getMap(CACHED_MAP);
        cachedMap.put("key", "value");
        cachedMap.put("key1", "value1");
        cachedMap.put("key2", "value2");
        cachedMap.put("key3", "value3");

        Thread.sleep(2000);
        for (int i = 0; i < n; i++) {

            Assert.assertTrue(h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost() > 0);

            Assert.assertTrue(h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost() == 0);

            Assert.assertTrue(h[i].getMap(CACHED_MAP).getLocalMapStats().getHeapCost() > 0);
        }

        // clear map
        binaryMap.clear();
        objectMap.clear();
        cachedMap.clear();

        Thread.sleep(2000);

        for (int i = 0; i < n; i++) {
            Assert.assertTrue(h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost() == 0);

            Assert.assertTrue(h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost() == 0);

            Assert.assertTrue(h[i].getMap(CACHED_MAP).getLocalMapStats().getHeapCost() == 0);
        }
    }

}
