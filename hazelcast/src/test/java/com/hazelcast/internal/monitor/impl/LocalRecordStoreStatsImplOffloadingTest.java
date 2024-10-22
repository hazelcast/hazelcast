/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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

import java.util.Map;

import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.assertj.core.api.Assertions.assertThat;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRecordStoreStatsImplOffloadingTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    String MAP_NAME = "map-name";

    private IMap<String, String> map;

    @Before
    public void setUp() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setMapStoreConfig(new MapStoreConfig()
                .setClassName(AMapStore.class.getName()));
        config.setMapConfigs(Map.of(MAP_NAME, mapConfig));

        HazelcastInstance instance = createHazelcastInstance(config);
        map = instance.getMap(MAP_NAME);
    }

    @Test
    public void delete_updates_last_update_time() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeUpdated(() -> map.delete("key:0"));
    }

    @Test
    public void delete_does_not_update_last_update_time_when_no_matching_key() {
        for (int i = 0; i < 10; i++) {
            map.set("key:" + i, "value:" + i);
        }

        assertLastUpdateTimeIsNotUpdated(() -> map.delete("key:100"));
    }

    private void assertLastUpdateTime(Runnable runnable, boolean updated) {
        long lastUpdateTimeBefore = map.getLocalMapStats().getLastUpdateTime();
        sleepAtLeastMillis(100);

        runnable.run();

        long lastUpdateTimeAfter = map.getLocalMapStats().getLastUpdateTime();
        if (updated) {
            assertThat(lastUpdateTimeAfter).isGreaterThan(lastUpdateTimeBefore);
        } else {
            assertThat(lastUpdateTimeAfter).isEqualTo(lastUpdateTimeBefore);
        }
    }

    private void assertLastUpdateTimeUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, true);
    }

    private void assertLastUpdateTimeIsNotUpdated(Runnable runnable) {
        assertLastUpdateTime(runnable, false);
    }
}
