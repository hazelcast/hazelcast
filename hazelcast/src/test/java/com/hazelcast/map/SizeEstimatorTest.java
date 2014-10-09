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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class SizeEstimatorTest extends HazelcastTestSupport {

    @Test
    public void smoke() throws InterruptedException {
        final SizeEstimatorTestMapBuilder<Long, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<Long, Long>();
        testMapBuilder.withNodeCount(1).withBackupCount(0).build();
        assertEquals(0, testMapBuilder.totalHeapCost());
    }

    @Test
    public void testSinglePut() throws InterruptedException {
        final long expectedPerEntryHeapCost = 172L;
        final SizeEstimatorTestMapBuilder<Integer, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<Integer, Long>();
        final IMap<Integer, Long> map = testMapBuilder.withNodeCount(1).withBackupCount(0).build();
        map.put(0, 10L);
        assertEquals(expectedPerEntryHeapCost, testMapBuilder.totalHeapCost());
    }

    @Test
    public void testExactHeapCostAfterUpdateWithMultipleBackupNodes() throws InterruptedException {
        final long expectedPerEntryHeapCost = 172L;
        final int putCount = 1;
        final int nodeCount = 1;
        final SizeEstimatorTestMapBuilder<Integer, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<Integer, Long>();
        final IMap<Integer, Long> map = testMapBuilder.withNodeCount(nodeCount).withBackupCount(nodeCount - 1).build();
        for (int i = 0; i < putCount; i++) {
            map.put(i, System.currentTimeMillis());
        }
        final long heapCost = testMapBuilder.totalHeapCost();
        assertEquals("Heap cost calculation is wrong!",
                expectedPerEntryHeapCost * putCount * nodeCount, heapCost);
    }


    @Test
    public void testPutRemoveWithTwoNodeOwnerAndBackup() throws InterruptedException {
        final String mapName = "default";
        final Config config = new Config();
        config.getMapConfig(mapName).setBackupCount(1).setInMemoryFormat(InMemoryFormat.BINARY);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance h[] = factory.newInstances(config);
        warmUpPartitions(h);
        //create map
        final IMap<String, String> map1 = h[0].getMap(mapName);
        final IMap<String, String> map2 = h[1].getMap(mapName);
        //calculate initial heap costs.
        long map1Cost = map1.getLocalMapStats().getHeapCost();
        long map2Cost = map2.getLocalMapStats().getHeapCost();
        //check initial costs if zero.
        assertEquals("map1 initial heap cost must be zero..." + map1Cost, 0, map1Cost);
        assertEquals("map2 initial heap cost must be zero..." + map2Cost, 0, map2Cost);
        //populate map
        map1.put("key", "value");
        //get sizes
        long map1Size = map1.size();
        long map2Size = map2.size();
        //check sizes
        assertEquals("map1 size must be one..." + map1Size, 1, map1Size);
        assertEquals("map2 size must be one..." + map2Size, 1, map2Size);
        //calculate costs
        map1Cost = map1.getLocalMapStats().getHeapCost();
        map2Cost = map2.getLocalMapStats().getHeapCost();
        //costs should not be zero.
        assertTrue("map1 cost should be greater than zero....: " + map1Cost, map1Cost > 0);
        assertTrue("map2 cost should be greater than zero.... : " + map2Cost, map2Cost > 0);
        // one map is backup. so backup & owner cost must be same.
        assertEquals(map1Cost, map2Cost);
        //remove key.
        map1.remove("key");
        //get sizes
        map1Size = map1.size();
        map2Size = map2.size();
        //check if sizes zero.
        assertEquals("map1 size must be zero..." + map1Size, 0, map1Size);
        assertEquals("map2 size must be zero..." + map2Size, 0, map2Size);
        map1Cost = map1.getLocalMapStats().getHeapCost();
        map2Cost = map2.getLocalMapStats().getHeapCost();
        //costs should be zero.
        assertTrue("map1 cost should zero....: " + map1Cost, map1Cost == 0);
        assertTrue("map2 cost should zero....: " + map2Cost, map2Cost == 0);
    }

    @Test
    public void testNearCache() throws InterruptedException {
        final String NO_NEAR_CACHED_MAP = "testIssue833";
        final String NEAR_CACHED_MAP = "testNearCache";

        final Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        config.getMapConfig(NEAR_CACHED_MAP).setNearCacheConfig(nearCacheConfig).setBackupCount(0);
        config.getMapConfig(NO_NEAR_CACHED_MAP).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance h[] = factory.newInstances(config);
        warmUpPartitions(h);

        final IMap<String, String> noNearCached = h[0].getMap(NO_NEAR_CACHED_MAP);
        for (int i = 0; i < 1000; i++) {
            noNearCached.put("key" + i, "value" + i);
        }

        final IMap<String, String> nearCachedMap = h[0].getMap(NEAR_CACHED_MAP);
        for (int i = 0; i < 1000; i++) {
            nearCachedMap.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            nearCachedMap.get("key" + i);
        }

        assertTrue(nearCachedMap.getLocalMapStats().getHeapCost() > noNearCached.getLocalMapStats().getHeapCost());
    }

    @Test
    public void testInMemoryFormats() throws InterruptedException {
        final String BINARY_MAP = "testBinaryFormat";
        final String OBJECT_MAP = "testObjectFormat";
        final Config config = new Config();
        config.getMapConfig(BINARY_MAP).
                setInMemoryFormat(InMemoryFormat.BINARY).setBackupCount(0);
        config.getMapConfig(OBJECT_MAP).
                setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0);

        final int n = 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);
        final HazelcastInstance[] h = factory.newInstances(config);
        warmUpPartitions(h);

        // populate map.
        final IMap<String, String> binaryMap = h[0].getMap(BINARY_MAP);
        for (int i = 0; i < 1000; i++) {
            binaryMap.put("key" + i, "value" + i);
        }

        final IMap<String, String> objectMap = h[0].getMap(OBJECT_MAP);
        for (int i = 0; i < 1000; i++) {
            objectMap.put("key" + i, "value" + i);
        }

        for (int i = 0; i < n; i++) {
            assertTrue(h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost() > 0);
            assertEquals(0, h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost());
        }

        // clear map
        binaryMap.clear();
        objectMap.clear();

        for (int i = 0; i < n; i++) {
            assertEquals(0, h[i].getMap(BINARY_MAP).getLocalMapStats().getHeapCost());
            assertEquals(0, h[i].getMap(OBJECT_MAP).getLocalMapStats().getHeapCost());
        }
    }

    private static class SizeEstimatorTestMapBuilder<K, V> {

        private HazelcastInstance[] nodes;

        private int nodeCount;

        private int backupCount;

        private String mapName = randomMapName("default");

        private InMemoryFormat inMemoryFormat = InMemoryFormat.BINARY;

        public SizeEstimatorTestMapBuilder() {
        }

        public SizeEstimatorTestMapBuilder<K, V> mapName(String mapName) {
            if (mapName == null) {
                throw new IllegalArgumentException("mapName is null");
            }
            this.mapName = mapName;
            return this;
        }

        public SizeEstimatorTestMapBuilder<K, V> withNodeCount(int nodeCount) {
            if (nodeCount < 1) {
                throw new IllegalArgumentException("nodeCount < 1");
            }
            this.nodeCount = nodeCount;
            return this;
        }

        public SizeEstimatorTestMapBuilder<K, V> withBackupCount(int backupCount) {
            if (backupCount < 0) {
                throw new IllegalArgumentException("backupCount < 1");
            }
            this.backupCount = backupCount;
            return this;
        }

        public SizeEstimatorTestMapBuilder<K, V> withInMemoryFormat(InMemoryFormat inMemoryFormat) {
            this.inMemoryFormat = inMemoryFormat;
            return this;
        }

        public IMap<K, V> build() {
            if (backupCount > nodeCount - 1) {
                throw new IllegalArgumentException("backupCount > nodeCount - 1");
            }
            final Config config = new Config();
            config.getMapConfig(mapName).setBackupCount(backupCount).setInMemoryFormat(inMemoryFormat);
            // nodes.
            final TestHazelcastInstanceFactory instanceFactory = new TestHazelcastInstanceFactory(nodeCount);
            nodes = instanceFactory.newInstances(config);
            return nodes[0].getMap(mapName);
        }

        public long totalHeapCost() {
            long heapCost = 0L;
            for (int i = 0; i < nodeCount; i++) {
                heapCost += nodes[i].getMap(mapName).getLocalMapStats().getHeapCost();
            }
            return heapCost;
        }

    }

}
