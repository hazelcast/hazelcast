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
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.PerIndexStats;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.IMap;
import com.hazelcast.query.impl.IndexCopyBehavior;
import com.hazelcast.query.impl.IndexRegistry;
import com.hazelcast.query.impl.IndexUtils;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;

import static com.hazelcast.config.MapConfig.DEFAULT_IN_MEMORY_FORMAT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITION_UPDATES_FINISHED;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITION_UPDATES_STARTED;
import static com.hazelcast.map.metrics.MetricTestUtils.assertAttributeEquals;
import static com.hazelcast.map.metrics.MetricTestUtils.buildMapIndexMetricName;
import static org.assertj.core.api.Assertions.assertThat;

@ParallelJVMTest
@QuickTest
class PartitionUpdateCountersTest
        extends HazelcastTestSupport {

    @Test
    void testCountersOnPublishedMetric() {
        Config config = MetricTestUtils.setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        HazelcastInstance hz = createHazelcastInstance(config);
        IMap<Integer, User> testMap = hz.getMap("testMap");
        for (int i = 0; i < 10; i++) {
            testMap.put(i, new User("" + i, ""));
        }
        long partitionCount = hz.getPartitionService().getPartitions().size();
        ObjectName indexName = buildMapIndexMetricName(hz, "testMap", "testIndex");
        assertAttributeEquals(partitionCount, indexName, MAP_METRIC_INDEX_PARTITION_UPDATES_STARTED);
        assertAttributeEquals(partitionCount, indexName, MAP_METRIC_INDEX_PARTITION_UPDATES_FINISHED);
    }

    @Test
    void testCountersOnPartitionIndexing() {
        testCounterOnPartitionChange(true);
    }

    @Test
    void testCountersOnPartitionUnindexing() {
        testCounterOnPartitionChange(false);
    }

    void testCounterOnPartitionChange(boolean indexing) {
        IndexRegistry indexRegistry = IndexRegistry.newBuilder(null, "test", new DefaultSerializationServiceBuilder().build(),
                IndexCopyBehavior.NEVER, DEFAULT_IN_MEMORY_FORMAT).global(true).statsEnabled(true).build();

        InternalIndex index = indexRegistry.addOrGetIndex(IndexUtils.createTestIndexConfig(IndexType.HASH, "abc"));
        PerIndexStats stats = index.getPerIndexStats();
        int changeCount = 5;
        for (int i = 0; i < changeCount; i++) {
            index.beginPartitionUpdate();
            assertThat(stats.getPartitionUpdatesStarted()).isEqualTo(i + 1);
            assertThat(stats.getPartitionUpdatesFinished()).isZero();
        }
        for (int i = 0; i < changeCount; i++) {
            if (indexing) {
                index.markPartitionAsIndexed(i);
            } else {
                index.markPartitionAsUnindexed(i);
            }
            assertThat(stats.getPartitionUpdatesStarted()).isEqualTo(5);
            assertThat(stats.getPartitionUpdatesFinished()).isEqualTo(i + 1);
        }
    }
}
