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

package com.hazelcast.map.impl;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.stream.Collectors;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test for {@link MapStatisticsAwareService}
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStatisticsAwareServiceTest extends HazelcastTestSupport {

    HazelcastInstance hz;
    IMap map;

    @Before
    public void setUp() throws Exception {
        hz = createHazelcastInstance();
        warmUpPartitions(hz);
        sleepSeconds(15);
        map = hz.getMap("map");
        sleepSeconds(5);
    }

    @Test
    public void getStats_returns_stats_object_when_map_is_empty() {
        assertStatsObjectCreated();
    }

    @Test
    public void getStats_returns_stats_object_when_map_is_not_empty() {
        map.put(1, 1);
        assertStatsObjectCreated();
    }

    private void assertStatsObjectCreated() {
        MapService mapService = getNodeEngineImpl(hz).getService(MapService.SERVICE_NAME);
        Map<String, LocalMapStats> mapStats = mapService.getStats();

        // then we obtain 1 local map stats instance
        int size = mapStats.size();
        if (size != 1) {
            String names = mapStats.keySet()
                    .stream().filter(name -> !name.equals("map")).collect(Collectors.joining(" "));
            assertEquals("Unexpected map statistics found: " + names, 1, size);
        }
        assertNotNull(mapStats.get("map"));

        hz.shutdown();
    }
}
