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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.metrics.managementcenter.ReadMetricsOperation;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadMetricsOperationTest extends HazelcastTestSupport {

    @Test
    public void testMetricsPresent_map() {
        Config config = new Config();

        HazelcastInstance hzInstance = createHazelcastInstance(config);
        IMap<Object, Object> map = hzInstance.getMap("map");
        map.put("key", "value");

        NodeEngineImpl nodeEngine = getNode(hzInstance).getNodeEngine();
        OperationServiceImpl operationService = nodeEngine.getOperationService();

        AtomicLong nextSequence = new AtomicLong();
        assertTrueEventually(() -> {
            long sequence = nextSequence.get();
            ReadMetricsOperation readMetricsOperation = new ReadMetricsOperation(sequence);
            InternalCompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future = operationService
                    .invokeOnTarget(MetricsService.SERVICE_NAME, readMetricsOperation, nodeEngine.getThisAddress());

            RingbufferSlice<Map.Entry<Long, byte[]>> ringbufferSlice = future.get();

            MetricsResultSet metricsResultSet = new MetricsResultSet(ringbufferSlice.nextSequence(), ringbufferSlice.elements());
            nextSequence.set(metricsResultSet.nextSequence());

            boolean mapMetric = false;
            List<Map.Entry<Long, byte[]>> collections = metricsResultSet.collections();
            MetricKeyConsumer metricConsumer = new MetricKeyConsumer();
            for (Map.Entry<Long, byte[]> entry : collections) {
                MetricsCompressor.extractMetrics(entry.getValue(), metricConsumer);
            }
            assertTrue(metricConsumer.mapMetric);
        });
    }

    private static class MetricKeyConsumer implements MetricConsumer {

        boolean mapMetric;

        @Override
        public void consumeLong(MetricDescriptor descriptor, long value) {
            mapMetric |= descriptor.metricString().contains("name=map")
                    & descriptor.metricString().contains("map.");
        }

        @Override
        public void consumeDouble(MetricDescriptor descriptor, double value) {
            mapMetric |= descriptor.metricString().contains("name=map")
                    & descriptor.metricString().contains("map.");
        }
    }
}
