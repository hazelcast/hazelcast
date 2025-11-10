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

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.monitor.impl.LocalIndexStatsImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.MigrationState;
import com.hazelcast.partition.ReplicaMigrationEvent;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.jupiter.api.Test;

import javax.management.ObjectName;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_INDEX_PARTITIONS_INDEXED;
import static com.hazelcast.map.metrics.MetricTestUtils.assertAttributeEquals;
import static com.hazelcast.map.metrics.MetricTestUtils.buildMapIndexMetricName;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

@ParallelJVMTest
@QuickTest
class PartitionsIndexedTest
        extends HazelcastTestSupport {

    @Test
    void testWithGracefulShutdown() {
        testImpl(true);
    }

    @Test
    void testWithUngracefulShutdown() {
        testImpl(false);
    }

    void testImpl(boolean shutdownGracefully) {
        Config config = MetricTestUtils.setRapidMetricsCollection(smallInstanceConfig()).addMapConfig(
                new MapConfig().setName("testMap").addIndexConfig(User.getIndexConfigOnUserId()));

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance hz0 = factory.newHazelcastInstance(config.setInstanceName("main"));
        getTestMap(hz0).put(0, new User("abc", "xyz"));
        assertPartitionsIndexedMatchesPartitionsOwned(hz0);
        CountDownLatch scaleUpMigrationLatch = getMigrationLatch(hz0);
        HazelcastInstance hz1 = factory.newHazelcastInstance(config.setInstanceName("other"));

        try {
            if (!scaleUpMigrationLatch.await(30, SECONDS)) {
                fail("Timed out waiting for scale up migration");
            }
            assertPartitionsIndexedMatchesPartitionsOwned(hz0);
            assertPartitionsIndexedMatchesPartitionsOwned(hz1);
            CountDownLatch scaleDownMigrationLatch = getMigrationLatch(hz0);
            if (shutdownGracefully) {
                hz1.getLifecycleService().shutdown();
            } else {
                hz1.getLifecycleService().terminate();
            }
            if (!scaleDownMigrationLatch.await(30, SECONDS)) {
                fail("Timed out waiting for scale down migration");
            }
            assertPartitionsIndexedMatchesPartitionsOwned(hz0);
        } catch (InterruptedException e) {
            fail("Test interrupted");
        }
    }

    void assertPartitionsIndexedMatchesPartitionsOwned(HazelcastInstance hz) {
        Member hzAsMember = hz.getCluster().getLocalMember();
        long ownedPartitions = hz.getPartitionService().getPartitions().stream().filter(p -> hzAsMember.equals(p.getOwner()))
                                 .count();
        LocalIndexStats stats = getTestMap(hz).getLocalMapStats().getIndexStats().get("testIndex");
        long partitionsCovered = ((LocalIndexStatsImpl) stats).getPartitionsIndexed();
        assertThat(partitionsCovered).isEqualTo(ownedPartitions);
        ObjectName metricName = buildMapIndexMetricName(hz, "testMap", "testIndex");
        assertAttributeEquals(ownedPartitions, metricName, MAP_METRIC_INDEX_PARTITIONS_INDEXED);
    }

    CountDownLatch getMigrationLatch(HazelcastInstance hz) {
        CountDownLatch latch = new CountDownLatch(1);
        hz.getPartitionService().addMigrationListener(new MigrationListener() {
            @Override
            public void migrationStarted(MigrationState state) {
            }

            @Override
            public void migrationFinished(MigrationState state) {
                latch.countDown();
            }

            @Override
            public void replicaMigrationCompleted(ReplicaMigrationEvent event) {
            }

            @Override
            public void replicaMigrationFailed(ReplicaMigrationEvent event) {
            }
        });
        return latch;
    }

    IMap<Integer, User> getTestMap(HazelcastInstance hz) {
        return hz.getMap("testMap");
    }
}
