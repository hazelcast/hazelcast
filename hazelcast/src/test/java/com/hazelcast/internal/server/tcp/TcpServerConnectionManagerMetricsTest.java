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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.OverridePropertyRule.set;
import static com.hazelcast.test.TestEnvironment.HAZELCAST_TEST_USE_NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpServerConnectionManagerMetricsTest extends HazelcastTestSupport {

    @Rule
    public final OverridePropertyRule overridePropertyRule = set(HAZELCAST_TEST_USE_NETWORK, "true");

    @Test
    public void testUnifiedEndpointManagerMetricsCollectedOnce() {
        CapturingCollector collector = new CapturingCollector();
        HazelcastInstance instance = createHazelcastInstance();

        getNodeEngineImpl(instance).getMetricsRegistry().collect(collector);

        // defined by ServerEndpointManager
        verifyCollectedOnce(collector, metricDescriptor(TCP_PREFIX_CONNECTION, "count"));
        // defined by ServerUnifiedEndpointManager
        verifyCollectedOnce(collector, metricDescriptor(TCP_PREFIX_CONNECTION, "clientCount"));
        verifyCollectedOnce(collector, metricDescriptor(TCP_PREFIX_CONNECTION, "textCount"));
    }

    private void verifyCollectedOnce(CapturingCollector collector, MetricDescriptor expectedDescriptor) {
        CapturingCollector.Capture capture = collector.captures().get(expectedDescriptor);
        assertNotNull(capture);
        assertEquals(1, capture.hits());
        assertInstanceOf(Long.class, capture.singleCapturedValue());
    }

    private MetricDescriptor metricDescriptor(String prefix, String metric) {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get()
                .withPrefix(prefix)
                .withMetric(metric)
                .withUnit(COUNT);
    }
}
