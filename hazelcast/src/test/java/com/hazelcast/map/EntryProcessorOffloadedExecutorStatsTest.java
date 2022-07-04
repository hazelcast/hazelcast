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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Offloadable;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR;
import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceBasicTest.assertMetricsCollected;
import static com.hazelcast.scheduledexecutor.impl.ScheduledExecutorServiceBasicTest.collectMetrics;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EntryProcessorOffloadedExecutorStatsTest extends HazelcastTestSupport {

    @Test
    public void offloaded_entry_processor_collects_executor_statistics_when_stats_enabled() {
        // run task
        String mapName = "map";
        Config config = smallInstanceConfig();
        config.getMapConfig(mapName)
                .setStatisticsEnabled(true);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(mapName);
        map.executeOnKey(1, new OneSecondSleepingEntryProcessor());

        // collect metrics
        Map<String, List<Long>> metrics
                = collectMetrics(MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR, instance);

        // check results
        assertMetricsCollected(metrics, 1000, 0,
                1, 1, 0, 1, 0);
    }

    @Test
    public void offloaded_entry_processor_collects_executor_statistics_when_stats_disabled() {
        // run task
        String mapName = "map";
        Config config = smallInstanceConfig();
        config.getMapConfig(mapName)
                .setStatisticsEnabled(false);

        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(mapName);
        map.executeOnKey(1, new OneSecondSleepingEntryProcessor());

        // collect metrics
        Map<String, List<Long>> metrics
                = collectMetrics(MAP_PREFIX_ENTRY_PROCESSOR_OFFLOADABLE_EXECUTOR, instance);

        // check results
        assertTrue("No metrics collection expected but " + metrics, metrics.isEmpty());
    }

    static class OneSecondSleepingEntryProcessor implements EntryProcessor, Offloadable {

        @Override
        public String getExecutorName() {
            return "STATS_OFFLOADED_EXECUTOR";
        }

        @Override
        public Object process(Map.Entry entry) {
            sleepSeconds(1);
            return null;
        }
    }

}
