/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.metrics;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEXES_SKIPPED_QUERY_COUNT;
import static com.hazelcast.map.metrics.MetricTestUtils.assertAttributeEquals;
import static com.hazelcast.map.metrics.MetricTestUtils.buildMapMetricName;
import static com.hazelcast.map.metrics.MetricTestUtils.setRapidMetricsCollection;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class IndexesSkippedQueryCountTest
        extends HazelcastTestSupport {

    record KeyDivisiblePredicate<V>(int divisor)
            implements Predicate<Integer, V> {
        @Override
        public boolean apply(Map.Entry<Integer, V> mapEntry) {
            return mapEntry.getKey() % divisor == 0;
        }
    }

    @Test
    void testIndexesSkipped() {
        Config config = setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<Integer, User> testMap = hz.getMap("testMap");
        for (int i = 0; i < 5; i++) {
            testMap.put(i, new User("" + i, ""));
        }

        assertThat(testMap.entrySet(new KeyDivisiblePredicate<>(5)).stream().map(Map.Entry::getValue).toList()).isEqualTo(
                List.of(new User("0", "")));

        LocalMapStatsImpl mapStats = (LocalMapStatsImpl) testMap.getLocalMapStats();
        assertThat(mapStats.getIndexesSkippedQueryCount()).isEqualTo(1);
        assertAttributeEquals(1L, buildMapMetricName(hz, "testMap"), MAP_METRIC_INDEXES_SKIPPED_QUERY_COUNT);
    }
}
