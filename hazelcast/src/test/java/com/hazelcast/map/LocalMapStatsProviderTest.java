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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsProviderTest extends HazelcastTestSupport {
    private static final String MAP_NAME = "test_map_1";
    private final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    //https://github.com/hazelcast/hazelcast/issues/11598
    @Test
    public void testRedundantPartitionMigrationWhenManagementCenterConfigured() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();

        //don't need start management center, just configure it
        final HazelcastInstance instance = factory.newHazelcastInstance(config);

        assertTrueEventually(() -> {
            ManagementCenterService mcs = getNode(instance).getManagementCenterService();
            assertNotNull(mcs);
        });

        assertTrueAllTheTime(() -> {
            ManagementCenterService mcs = getNode(instance).getManagementCenterService();
            mcs.getTimedMemberStateJson();

            //check partition migration triggered or not
            long partitionStateStamp = getNode(instance).getPartitionService().getPartitionStateStamp();
            assertEquals(0, partitionStateStamp);
        }, 5);
    }

    @Test
    public void testLiteMembersProvideLocalMapStatsForInvocations() {
        // Create a regular member which will own partitions and execute MapOperations
        Config nonLiteConfig = createMetricsBasedConfig();
        HazelcastInstance hzFull = factory.newHazelcastInstance(nonLiteConfig);
        // Create a Lite member which will only invoke MapOperations
        Config liteConfig = createMetricsBasedConfig().setLiteMember(true);
        HazelcastInstance hzLite = factory.newHazelcastInstance(liteConfig);

        waitAllForSafeState(hzFull, hzLite);

        // From the Lite member, access and put data into a map
        IMap<Object, Object> map1 = hzLite.getMap(MAP_NAME);
        map1.put("myKey", "myValue");

        // Confirm 1 putCount metric was collected for the Lite member
        assertMapPutCountMetric(hzLite, 1);
        // Confirm no putCount metric was collected for the full member
        assertMapPutCountMetric(hzFull, 0);

        // Destroy the map and ensure metrics are cleaned up
        map1.destroy();
        MapService mapService = getNode(hzLite).getNodeEngine().getService(MapService.SERVICE_NAME);
        LocalMapStatsProvider statsProvider = mapService.getMapServiceContext().getLocalMapStatsProvider();
        assertFalse(statsProvider.hasLocalMapStatsImpl(MAP_NAME));
    }

    private void assertMapPutCountMetric(HazelcastInstance instance, int expected) {
        MetricsRegistry metricsRegistry = getNode(instance).getNodeEngine().getMetricsRegistry();
        MapPutCountMetricsCollector metricsCollector = new MapPutCountMetricsCollector();
        metricsRegistry.collect(metricsCollector);
        assertEquals(expected, metricsCollector.totalCollected.get());
    }

    private Config createMetricsBasedConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.setClusterName("dev");
        config.getMetricsConfig().setEnabled(true);
        MapConfig map = config.getMapConfig(MAP_NAME);
        map.setStatisticsEnabled(true);
        map.setPerEntryStatsEnabled(true);
        return config;
    }

    private static class MapPutCountMetricsCollector implements MetricsCollector {
        final AtomicInteger totalCollected = new AtomicInteger(0);

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            if (descriptor.metric().equals("putCount")) {
                String discriminator = descriptor.discriminatorValue();
                String prefix = descriptor.prefix();
                if (prefix != null && prefix.equals("map") && discriminator != null && discriminator.equals(MAP_NAME)) {
                    totalCollected.addAndGet((int) value);
                }
            }
        }

        @Override
        public void collectDouble(MetricDescriptor descriptor, double value) {
        }

        @Override
        public void collectException(MetricDescriptor descriptor, Exception e) {
        }

        @Override
        public void collectNoValue(MetricDescriptor descriptor) {
        }
    }
}
