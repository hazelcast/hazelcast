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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.config.Config;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRecordStoreStatsImplTest extends HazelcastTestSupport {

    private IMap<String, String> map;

    @Before
    public void setUp() throws Exception {
        map = createHazelcastInstance(getConfig()).getMap("test-map");
    }

    @Override
    protected Config getConfig() {
        return smallInstanceConfig();
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
