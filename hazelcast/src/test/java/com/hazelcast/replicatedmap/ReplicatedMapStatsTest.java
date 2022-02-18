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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReplicatedMapStatsTest extends HazelcastTestSupport {

    private static final int OPERATION_COUNT = 10;
    private static final int DEFAULT_PARTITION_COUNT = Integer.valueOf(PARTITION_COUNT.getDefaultValue());
    private HazelcastInstance instance;
    private String replicatedMapName = "replicatedMap";

    @Before
    public void setUp() {
        instance = createHazelcastInstance();
    }

    protected <K, V> ReplicatedMap<K, V> getReplicatedMap() {
        warmUpPartitions(instance);
        return instance.getReplicatedMap(replicatedMapName);
    }

    protected LocalReplicatedMapStats getReplicatedMapStats() {
        return instance.getReplicatedMap(replicatedMapName).getReplicatedMapStats();
    }

    protected HazelcastInstance getInstance() {
        return instance;
    }

    @Test
    public void testGetOperationCount() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        replicatedMap.put(1, 1);
        int count = OPERATION_COUNT;
        for (int i = 0; i < count; i++) {
            replicatedMap.get(1);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(count, stats.getGetOperationCount());
    }

    @Test
    public void testPutOperationCount() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        int count = OPERATION_COUNT;
        for (int i = 0; i < count; i++) {
            replicatedMap.put(i, i);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(count, stats.getPutOperationCount());
    }

    @Test
    public void testRemoveOperationCount() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        int count = OPERATION_COUNT;
        for (int i = 0; i < count; i++) {
            replicatedMap.put(i, i);
            replicatedMap.remove(i);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(count, stats.getRemoveOperationCount());
    }

    @Test
    public void testHitsGenerated() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        for (int i = 0; i < OPERATION_COUNT; i++) {
            replicatedMap.put(i, i);
            replicatedMap.get(i);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT, stats.getHits());
    }

    @Test
    public void testPutAndHitsGenerated() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        for (int i = 0; i < OPERATION_COUNT; i++) {
            replicatedMap.put(i, i);
            replicatedMap.get(i);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT, stats.getHits());
    }

    @Test
    public void testGetAndHitsGenerated() {
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        for (int i = 0; i < OPERATION_COUNT; i++) {
            replicatedMap.put(i, i);
            replicatedMap.get(i);
        }
        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT, stats.getHits());
    }

    @Test
    public void testHitsGenerated_updatedConcurrently() {
        final ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        final int actionCount = OPERATION_COUNT;
        for (int i = 0; i < actionCount; i++) {
            replicatedMap.put(i, i);
            replicatedMap.get(i);
        }
        final LocalReplicatedMapStats stats = getReplicatedMapStats();
        final long initialHits = stats.getHits();

        new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < actionCount; i++) {
                    replicatedMap.get(i);
                }
                getReplicatedMapStats(); // causes the local stats object to update
            }
        }).start();

        assertEquals(actionCount, initialHits);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(actionCount * 2, stats.getHits());
            }
        });
    }

    @Test
    public void testLastAccessTime() {
        final long startTime = Clock.currentTimeMillis();
        ReplicatedMap<String, String> replicatedMap = getReplicatedMap();
        String key = "key";
        replicatedMap.put(key, "value");
        replicatedMap.get(key);
        long lastAccessTime = getReplicatedMapStats().getLastAccessTime();

        assertTrue(lastAccessTime >= startTime);
    }

    @Test
    public void testLastAccessTime_updatedConcurrently() {
        final long startTime = Clock.currentTimeMillis();
        final ReplicatedMap<String, String> map = getReplicatedMap();
        final String key = "key";
        map.put(key, "value");
        map.get(key);
        final LocalReplicatedMapStats stats = getReplicatedMapStats();
        final long lastAccessTime = stats.getLastAccessTime();

        new Thread(new Runnable() {
            @Override
            public void run() {
                sleepAtLeastMillis(1);
                map.get(key);
                map.getReplicatedMapStats(); // causes the local stats object to update
            }
        }).start();

        assertTrue(lastAccessTime >= startTime);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(stats.getLastAccessTime() >= lastAccessTime);
            }
        });
    }

    @Test
    public void testLastUpdateTime() {
        final long startTime = Clock.currentTimeMillis();

        ReplicatedMap<String, String> replicatedMap = getReplicatedMap();
        String key = "key";
        replicatedMap.put(key, "value");

        long lastUpdateTime = getReplicatedMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime >= startTime);

        sleepAtLeastMillis(5);
        replicatedMap.put(key, "value2");
        long lastUpdateTime2 = getReplicatedMapStats().getLastUpdateTime();
        assertTrue(lastUpdateTime2 >= lastUpdateTime);
    }

    @Test
    public void testLastUpdateTime_updatedConcurrently() {
        final long startTime = Clock.currentTimeMillis();
        final ReplicatedMap<String, String> map = getReplicatedMap();

        final String key = "key";
        map.put(key, "value");

        final LocalReplicatedMapStats stats = getReplicatedMapStats();
        final long lastUpdateTime = stats.getLastUpdateTime();

        new Thread(new Runnable() {
            @Override
            public void run() {
                sleepAtLeastMillis(1);
                map.put(key, "value2");
                getReplicatedMapStats(); // causes the local stats object to update
            }
        }).start();

        assertTrue(lastUpdateTime >= startTime);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(stats.getLastUpdateTime() >= lastUpdateTime);
            }
        });
    }

    @Test
    public void testPutOperationCount_afterPutAll() {
        Map<Integer, Integer> map = new HashMap<Integer, Integer>();
        for (int i = 1; i <= OPERATION_COUNT; i++) {
            map.put(i, i);
        }
        ReplicatedMap<Integer, Integer> replicatedMap = getReplicatedMap();
        replicatedMap.putAll(map);
        final LocalReplicatedMapStats stats = getReplicatedMapStats();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(OPERATION_COUNT, stats.getPutOperationCount());
            }
        });
    }

    @Test
    public void testOtherOperationCount_containsKey() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsKey(i);
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_entrySet() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.entrySet();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_keySet() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.keySet();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_values() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.values();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_clear() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.clear();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_containsValue() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsValue(1);
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_isEmpty() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.isEmpty();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_size() {
        ReplicatedMap<Integer, Integer> map = getReplicatedMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.size();
        }

        LocalReplicatedMapStats stats = getReplicatedMapStats();
        assertEquals(OPERATION_COUNT * DEFAULT_PARTITION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testEmptyStatsIfDisabled() {
        String name = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        replicatedMapConfig.setStatisticsEnabled(false);
        getInstance().getConfig().addReplicatedMapConfig(replicatedMapConfig);

        ReplicatedMap<Integer, Integer> replicatedMap = getInstance().getReplicatedMap(name);
        replicatedMap.put(1, 1);
        replicatedMap.get(1);

        LocalReplicatedMapStats stats = replicatedMap.getReplicatedMapStats();
        assertEquals(0, stats.getGetOperationCount());
        assertEquals(0, stats.getPutOperationCount());
    }

    @Test
    public void testNoObjectGenerationIfStatsDisabled() {
        String name = randomMapName();
        ReplicatedMapConfig replicatedMapConfig = new ReplicatedMapConfig();
        replicatedMapConfig.setName(name);
        replicatedMapConfig.setStatisticsEnabled(false);
        getInstance().getConfig().addReplicatedMapConfig(replicatedMapConfig);

        ReplicatedMap<Integer, Integer> replicatedMap = getInstance().getReplicatedMap(name);
        LocalReplicatedMapStats stats = replicatedMap.getReplicatedMapStats();
        LocalReplicatedMapStats stats2 = replicatedMap.getReplicatedMapStats();
        LocalReplicatedMapStats stats3 = replicatedMap.getReplicatedMapStats();
        assertSame(stats, stats2);
        assertSame(stats2, stats3);
    }
}
