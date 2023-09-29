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
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.IMap;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_EVICTION_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_EXPIRATION_COUNT;
import static com.hazelcast.map.impl.eviction.MapClearExpiredRecordsTask.PROP_TASK_PERIOD_SECONDS;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.OverridePropertyRule.set;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalRecordStoreStatsFailoverTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overrideTaskSecondsRule
            = set(PROP_TASK_PERIOD_SECONDS, String.valueOf(1));

    private static final String DEFAULT_MAP_NAME = "default";
    private static final String EVICTABLE_MAP_NAME = "evictable-map-name";

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        MapConfig mapConfig = config.getMapConfig(EVICTABLE_MAP_NAME);
        mapConfig.getEvictionConfig().setSize(1_000)
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE);
        return config;
    }

    @Test
    @Category(SlowTest.class)
    public void eviction_and_expiration_counts_preserved_when_node_bouncing() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        AtomicBoolean stop = new AtomicBoolean();

        spawn(() -> {
            while (!stop.get()) {
                HazelcastInstance instance3 = factory.newHazelcastInstance(config);
                sleepSeconds(2);
                instance3.shutdown();
            }
        });

        AtomicInteger putCount = new AtomicInteger();

        // insert entries
        spawn(() -> {
            IMap map = instance1.getMap(EVICTABLE_MAP_NAME);
            while (!stop.get()) {
                map.set(putCount.incrementAndGet(), "value", 1, TimeUnit.SECONDS);
            }
        });

        sleepSeconds(20);
        stop.set(true);

        assertTrueEventually(() -> {
            IMap map1 = instance1.getMap(EVICTABLE_MAP_NAME);
            LocalMapStats localMapStats1 = map1.getLocalMapStats();
            long evictionCount1 = localMapStats1.getEvictionCount();
            long expirationCount1 = localMapStats1.getExpirationCount();
            long ownedEntryCount1 = localMapStats1.getOwnedEntryCount();

            IMap map2 = instance2.getMap(EVICTABLE_MAP_NAME);
            LocalMapStats localMapStats2 = map2.getLocalMapStats();
            long evictionCount2 = localMapStats2.getEvictionCount();
            long expirationCount2 = localMapStats2.getExpirationCount();
            long ownedEntryCount2 = localMapStats2.getOwnedEntryCount();

            assertEquals(putCount.get(),
                    evictionCount1 + evictionCount2
                            + expirationCount1 + expirationCount2
                            + ownedEntryCount1 + ownedEntryCount2);
        });
    }

    @Test
    public void expiration_metrics_correct_after_expiration() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        MetricsRegistry registry1 = getNode(instance1).nodeEngine.getMetricsRegistry();
        MetricsRegistry registry2 = getNode(instance2).nodeEngine.getMetricsRegistry();

        IMap map1 = instance1.getMap(DEFAULT_MAP_NAME);
        IMap map2 = instance2.getMap(DEFAULT_MAP_NAME);

        int putCount = 1_000;
        for (int i = 0; i < putCount; i++) {
            map1.set(i, "value", 1, TimeUnit.SECONDS);
        }

        LocalMapStats localMapStats1 = map1.getLocalMapStats();
        LocalMapStats localMapStats2 = map2.getLocalMapStats();

        assertTrueEventually(() -> {
            long expirationCount = localMapStats1.getExpirationCount()
                    + localMapStats2.getExpirationCount();

            ProbeCatcher probeCatcher = new ProbeCatcher();
            registry1.collect(probeCatcher);
            registry2.collect(probeCatcher);

            assertEquals(expirationCount, probeCatcher.expirationCount);
            assertEquals(putCount, probeCatcher.expirationCount);
            assertEquals(0L, probeCatcher.evictionCount);
        });
    }

    @Test
    public void eviction_metrics_correct_after_eviction() {
        Config config = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        MetricsRegistry registry1 = getNode(instance1).nodeEngine.getMetricsRegistry();
        MetricsRegistry registry2 = getNode(instance2).nodeEngine.getMetricsRegistry();

        IMap map1 = instance1.getMap(EVICTABLE_MAP_NAME);
        IMap map2 = instance2.getMap(EVICTABLE_MAP_NAME);
        for (int i = 0; i < 10_000; i++) {
            map1.set(i, "value");
        }

        LocalMapStats localMapStats1 = map1.getLocalMapStats();
        LocalMapStats localMapStats2 = map2.getLocalMapStats();

        assertTrueEventually(() -> {
            long evictionCount = localMapStats1.getEvictionCount()
                    + localMapStats2.getEvictionCount();

            ProbeCatcher probeCatcher = new ProbeCatcher();
            registry1.collect(probeCatcher);
            registry2.collect(probeCatcher);

            assertEquals(evictionCount, probeCatcher.evictionCount);
        });
    }

    static class ProbeCatcher implements MetricsCollector {
        private long evictionCount;
        private long expirationCount;

        ProbeCatcher() {
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            String name = descriptor.toString();

            if (name.contains(MAP_METRIC_EVICTION_COUNT)) {
                evictionCount += value;
                return;
            }

            if (name.contains(MAP_METRIC_EXPIRATION_COUNT)) {
                expirationCount += value;
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
