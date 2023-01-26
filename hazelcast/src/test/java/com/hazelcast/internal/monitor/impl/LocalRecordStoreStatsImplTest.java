/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRecordStoreStatsImplTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    String MAP_NAME = "map-name";
    String EVICTABLE_MAP_NAME = "evictable-map-name";

    private IMap<String, String> map;
    private IMap<String, String> evictableMap;

    @Before
    public void setUp() throws Exception {
        Config config = getConfig();
        HazelcastInstance instance = createHazelcastInstance(config);

        map = instance.getMap(MAP_NAME);
        evictableMap = instance.getMap(EVICTABLE_MAP_NAME);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        MapConfig mapConfig = config.getMapConfig(EVICTABLE_MAP_NAME);
        mapConfig.getEvictionConfig().setSize(1_000)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        return config;
    }

    /**
     * Since this is an explicit evict triggered by the user we
     * don't expect to see eviction and expiration stats changed.
     * <p>
     * Only internal system should trigger
     * eviction and expiration stats.
     */
    @Test
    public void map_evict_does_not_change_eviction_and_expiration_stats() {
        for (int i = 0; i < 100; i++) {
            map.set(randomString(), randomString());
        }

        Iterator<String> iterator = map.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            map.evict(key);
        }

        LocalMapStats localMapStats = map.getLocalMapStats();
        long evictionCount = localMapStats.getEvictionCount();
        long expirationCount = localMapStats.getExpirationCount();

        assertEquals(0, evictionCount);
        assertEquals(0, expirationCount);
    }

    @Test
    public void eviction_count_increased_after_eviction() {
        for (int i = 0; i < 10_000; i++) {
            evictableMap.set(randomString(), randomString());
        }

        LocalMapStats localMapStats = evictableMap.getLocalMapStats();
        long evictionCount = localMapStats.getEvictionCount();
        long expirationCount = localMapStats.getExpirationCount();

        assertTrue(evictionCount > 9000);
        assertEquals(0, expirationCount);
    }

    @Test
    public void expiration_count_increases_after_expiration() {
        for (int i = 0; i < 1_00; i++) {
            map.set(randomString(), randomString(), 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(() -> assertEquals(0, map.size()));

        LocalMapStats localMapStats = map.getLocalMapStats();
        assertEquals(1_00, localMapStats.getExpirationCount());
        assertEquals(0, localMapStats.getEvictionCount());
    }

    @Test
    public void remove_updates_last_update_time() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeUpdated(() -> map.remove("key:0"));
    }

    @Test
    public void remove_does_not_update_last_update_time_when_no_matching_key() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeIsNotUpdated(() -> map.remove("key:100"));
    }

    @Test
    public void remove_if_same_updates_last_update_time() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeUpdated(() -> map.remove("key:0", "value:0"));
    }

    @Test
    public void remove_if_same_does_not_update_last_update_time_when_no_matching_key() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeIsNotUpdated(() -> map.remove("key:100", "value:100"));
    }

    @Test
    public void remove_if_same_does_not_update_last_update_time_when_no_matching_value() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeIsNotUpdated(() -> map.remove("key:0", "value:100"));
    }

    @Test
    public void clear_updates_last_update_time() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeUpdated(() -> map.clear());
    }

    @Test
    public void clear_does_not_update_last_update_time_when_map_is_empty() {
        assertLastUpdateTimeIsNotUpdated(() -> map.clear());
    }

    private void assertLastUpdateTimeUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, true);
    }

    private void assertLastUpdateTime(Runnable runnable, boolean updated) {
        long lastUpdateTimeBefore = map.getLocalMapStats().getLastUpdateTime();
        sleepAtLeastMillis(10);

        runnable.run();

        long lastUpdateTimeAfter = map.getLocalMapStats().getLastUpdateTime();
        if (updated) {
            assertTrue(lastUpdateTimeAfter > lastUpdateTimeBefore);
        } else {
            assertTrue(lastUpdateTimeAfter == lastUpdateTimeBefore);
        }
    }

    private void assertLastUpdateTimeIsNotUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, false);
    }
}
