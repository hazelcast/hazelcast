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

package com.hazelcast.map.impl.mapstore.offload;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.util.MutableLong;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapServiceContext;
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
import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapStoreForceOffloadAllOperationTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "default-map";
    private static final int INSTANCE_COUNT = 1;

    private IMap<String, String> map;
    private MetricsRegistry registry;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(INSTANCE_COUNT);
        Config config = getConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        registry = getNode(instance).nodeEngine.getMetricsRegistry();
        map = instance.getMap(MAP_NAME);
    }

    protected Config getConfig() {
        return super.smallInstanceConfigWithoutJetAndMetrics()
                .setProperty(MapServiceContext.FORCE_OFFLOAD_ALL_OPERATIONS.getName(), "true");
    }

    @Test
    public void metrics_show_offloaded_operation_count_when_forced_offload_enabled() {
        int opCount = 10_000;

        List<Future> futures = new ArrayList<>(opCount);
        for (int i = 0; i < opCount; i++) {
            futures.add(map.submitToKey(Integer.toString(i), (EntryProcessor) entry -> {
                // mimic slow execution
                sleepMillis(10);
                return null;
            }).toCompletableFuture());
        }

        MutableLong observedOffloadedOpCount = new MutableLong();

        assertTrueEventually(() -> {
            ProbeCatcher mapWithMapStore = new ProbeCatcher();
            registry.collect(mapWithMapStore);
            long metricsOffloadedOpCount = observedOffloadedOpCount.addAndGet(mapWithMapStore.length);
            assertTrue(format("Found %d metricsOffloadedOpCount", metricsOffloadedOpCount),
                    metricsOffloadedOpCount > 0);
        });
    }

    static class ProbeCatcher implements MetricsCollector {

        private long length;

        ProbeCatcher() {
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
