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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.MAP_METRIC_MAP_STORE_WAITING_TO_BE_PROCESSED_COUNT;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreOffloadedOperationMetricsTest extends HazelcastTestSupport {

    private static final String MAP_WITH_MAP_STORE_NAME = "map-with-map-store";
    private static final String MAP_WITHOUT_MAP_STORE_NAME = "no-map-store-map";
    private static final int INSTANCE_COUNT = 1;

    private IMap<String, String> mapWithMapStore;
    private IMap<String, String> mapWithoutMapStore;
    private MetricsRegistry registry;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        Config config = getConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        registry = getNode(instance).nodeEngine.getMetricsRegistry();
        mapWithMapStore = instance.getMap(MAP_WITH_MAP_STORE_NAME);
        mapWithoutMapStore = instance.getMap(MAP_WITHOUT_MAP_STORE_NAME);
    }

    protected Config getConfig() {
        Config config = super.getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapStoreAdapter<String, String>() {

            @Override
            public String load(String key) {
                // mimic a slow map loader
                sleepMillis(100);
                return randomString();
            }
        });
        config.getMapConfig(MAP_WITH_MAP_STORE_NAME)
                .setMapStoreConfig(mapStoreConfig);
        return config;
    }

    @Test
    public void test_metrics_show_zero_offloaded_operation_after_methods_return() {
        int opCount = 1_000;
        List<Future> futures = new ArrayList<>(opCount * 2);
        for (int i = 0; i < opCount; i++) {
            futures.add(mapWithMapStore.setAsync(Integer.toString(i), Integer.toString(i)).toCompletableFuture());
            futures.add(mapWithoutMapStore.setAsync(Integer.toString(i), Integer.toString(i)).toCompletableFuture());
        }

        sleepSeconds(2);

        assertTrueEventually(() -> {
            ProbeCatcher mapWithMapStore = new ProbeCatcher(MAP_WITH_MAP_STORE_NAME);
            ProbeCatcher mapWithoutMapStore = new ProbeCatcher(MAP_WITHOUT_MAP_STORE_NAME);

            registry.collect(mapWithMapStore);
            registry.collect(mapWithoutMapStore);

            assertEquals(0L, mapWithMapStore.length);
            assertEquals(0L, mapWithoutMapStore.length);
        });
    }

    static class ProbeCatcher implements MetricsCollector {

        private long length;
        private final String mapName;

        ProbeCatcher(String mapName) {
            this.mapName = mapName;
        }

        @Override
        public void collectLong(MetricDescriptor descriptor, long value) {
            String name = descriptor.toString();
            if (name.contains(MAP_METRIC_MAP_STORE_WAITING_TO_BE_PROCESSED_COUNT)) {
                length += value;
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
