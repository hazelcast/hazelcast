/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.util.JVMUtil;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryCostEstimatorTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public boolean perEntryStatsEnabled;

    @Parameterized.Parameters(name = "perEntryStatsEnabled:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {true},
                {false},
        });
    }

    protected TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

    public static final int ENTRY_COST_IN_BYTES_WHEN_STATS_OFF = getExpectedCostInBytes(false);
    public static final int ENTRY_COST_IN_BYTES_WHEN_STATS_ON = getExpectedCostInBytes(true);

    // values represent the cost when
    // perEntryStatsEnabled is false(default value).
    private static int getExpectedCostInBytes(boolean perEntryStatsEnabled) {
        if (JVMUtil.is32bitJVM() && JVMUtil.isCompressedOops()) {
            return perEntryStatsEnabled ? 140 : 116;
        }

        if (JVMUtil.isCompressedOops()) {
            return perEntryStatsEnabled ? 152 : 128;
        }

        return perEntryStatsEnabled ? 196 : 172;
    }

    private long getExpectedCost() {
        return perEntryStatsEnabled
                ? ENTRY_COST_IN_BYTES_WHEN_STATS_ON : ENTRY_COST_IN_BYTES_WHEN_STATS_OFF;
    }

    @Test
    public void smoke() {
        SizeEstimatorTestMapBuilder<Long, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<>(factory);
        testMapBuilder.withNodeCount(1).withBackupCount(0).build(getConfig());
        assertEquals(0, testMapBuilder.totalHeapCost());
    }

    @Test
    public void testSinglePut() {
        SizeEstimatorTestMapBuilder<Integer, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<>(factory);
        IMap<Integer, Long> map = testMapBuilder.withNodeCount(1).withBackupCount(0).build(getConfig());
        map.put(0, 10L);
        assertEquals(getExpectedCost(), testMapBuilder.totalHeapCost());
    }

    @Test
    public void testExactHeapCostAfterUpdateWithMultipleBackupNodes() {
        int putCount = 1;
        int nodeCount = 1;
        SizeEstimatorTestMapBuilder<Integer, Long> testMapBuilder = new SizeEstimatorTestMapBuilder<>(factory);
        IMap<Integer, Long> map = testMapBuilder.withNodeCount(nodeCount).withBackupCount(nodeCount - 1).build(getConfig());
        for (int i = 0; i < putCount; i++) {
            map.put(i, System.currentTimeMillis());
        }
        long heapCost = testMapBuilder.totalHeapCost();
        assertEquals("Heap cost calculation is wrong!", getExpectedCost() * putCount * nodeCount, heapCost);
    }

    @Test
    public void testPutRemoveWithTwoNodeOwnerAndBackup() {
        String name = randomString();
        Config config = getConfig();
        config.getMapConfig(name).setBackupCount(1);
        HazelcastInstance[] h = factory.newInstances(config);
        warmUpPartitions(h);

        // create map
        IMap<String, String> map1 = h[0].getMap(name);
        IMap<String, String> map2 = h[1].getMap(name);

        // calculate initial heap costs
        long map1Cost = map1.getLocalMapStats().getHeapCost();
        long map2Cost = map2.getLocalMapStats().getHeapCost();

        // check initial costs if zero
        assertEquals("map1 initial heap cost must be zero..." + map1Cost, 0, map1Cost);
        assertEquals("map2 initial heap cost must be zero..." + map2Cost, 0, map2Cost);

        // populate map
        map1.put("key", "value");

        // get sizes
        long map1Size = map1.size();
        long map2Size = map2.size();

        // check sizes
        assertEquals("map1 size must be one..." + map1Size, 1, map1Size);
        assertEquals("map2 size must be one..." + map2Size, 1, map2Size);

        // calculate costs
        map1Cost = map1.getLocalMapStats().getHeapCost();
        map2Cost = map2.getLocalMapStats().getHeapCost();

        // costs should not be zero
        assertTrue("map1 cost should be greater than zero....: " + map1Cost, map1Cost > 0);
        assertTrue("map2 cost should be greater than zero....: " + map2Cost, map2Cost > 0);

        // one map is backup, so backup & owner cost must be same
        assertEquals(map1Cost, map2Cost);

        // remove key
        map1.remove("key");

        // get sizes
        map1Size = map1.size();
        map2Size = map2.size();

        // check if sizes are zero
        assertEquals("map1 size must be zero..." + map1Size, 0, map1Size);
        assertEquals("map2 size must be zero..." + map2Size, 0, map2Size);
        map1Cost = map1.getLocalMapStats().getHeapCost();
        map2Cost = map2.getLocalMapStats().getHeapCost();

        // costs should be zero
        assertTrue("map1 cost should zero....: " + map1Cost, map1Cost == 0);
        assertTrue("map2 cost should zero....: " + map2Cost, map2Cost == 0);
    }

    @Test
    public void testNearCache() {
        String noNearCacheMapName = randomString();
        String nearCachedMapName = randomString();

        Config config = getConfig();
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.BINARY);
        config.getMapConfig(nearCachedMapName).setBackupCount(0).setNearCacheConfig(nearCacheConfig);
        config.getMapConfig(noNearCacheMapName).setBackupCount(0);

        HazelcastInstance[] h = factory.newInstances(config);
        warmUpPartitions(h);

        IMap<String, String> noNearCached = h[0].getMap(noNearCacheMapName);
        for (int i = 0; i < 1000; i++) {
            noNearCached.put("key" + i, "value" + i);
        }

        IMap<String, String> nearCachedMap = h[0].getMap(nearCachedMapName);
        for (int i = 0; i < 1000; i++) {
            nearCachedMap.put("key" + i, "value" + i);
        }

        for (int i = 0; i < 1000; i++) {
            nearCachedMap.get("key" + i);
        }

        assertTrue(nearCachedMap.getLocalMapStats().getHeapCost() > noNearCached.getLocalMapStats().getHeapCost());
    }

    @Test
    public void testInMemoryFormats() {
        String BINARY_MAP = "testBinaryFormat";
        String OBJECT_MAP = "testObjectFormat";
        Config config = new Config();
        config.getMapConfig(BINARY_MAP).setInMemoryFormat(InMemoryFormat.BINARY).setBackupCount(0);
        config.getMapConfig(OBJECT_MAP).setInMemoryFormat(InMemoryFormat.OBJECT).setBackupCount(0);

        int n = 2;

        HazelcastInstance[] h = factory.newInstances(config);
        warmUpPartitions(h);

        // populate map
        IMap<String, String> binaryMap = h[0].getMap(BINARY_MAP);
        for (int i = 0; i < 1000; i++) {
            binaryMap.put("key" + i, "value" + i);
        }

        IMap<String, String> objectMap = h[0].getMap(OBJECT_MAP);
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

    private class SizeEstimatorTestMapBuilder<K, V> {

        private String mapName = randomMapName("default");
        private HazelcastInstance[] nodes;
        private int nodeCount;
        private int backupCount;
        private TestHazelcastInstanceFactory instanceFactory;

        SizeEstimatorTestMapBuilder(TestHazelcastInstanceFactory factory) {
            this.instanceFactory = factory;
        }

        SizeEstimatorTestMapBuilder<K, V> mapName(String mapName) {
            if (mapName == null) {
                throw new IllegalArgumentException("mapName is null");
            }
            this.mapName = mapName;
            return this;
        }

        SizeEstimatorTestMapBuilder<K, V> withNodeCount(int nodeCount) {
            if (nodeCount < 1) {
                throw new IllegalArgumentException("nodeCount < 1");
            }
            this.nodeCount = nodeCount;
            this.nodes = new HazelcastInstance[nodeCount];
            return this;
        }

        SizeEstimatorTestMapBuilder<K, V> withBackupCount(int backupCount) {
            if (backupCount < 0) {
                throw new IllegalArgumentException("backupCount < 1");
            }
            this.backupCount = backupCount;
            return this;
        }

        IMap<K, V> build(Config config) {
            if (backupCount > nodeCount - 1) {
                throw new IllegalArgumentException("backupCount > nodeCount - 1");
            }
            config.getMapConfig(mapName).setBackupCount(backupCount)
                    .setPerEntryStatsEnabled(perEntryStatsEnabled);
            nodes = instanceFactory.newInstances(config, nodeCount);
            return nodes[0].getMap(mapName);
        }

        long totalHeapCost() {
            long heapCost = 0L;
            for (int i = 0; i < nodeCount; i++) {
                heapCost += nodes[i].getMap(mapName).getLocalMapStats().getHeapCost();
            }
            return heapCost;
        }
    }
}
