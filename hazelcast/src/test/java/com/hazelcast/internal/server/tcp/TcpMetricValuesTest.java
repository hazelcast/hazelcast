/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_BINDADDRESS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_DISCRIMINATOR_ENDPOINT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_ENDPOINT_MANAGER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX_CONNECTION;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class TcpMetricValuesTest {

    @Before
    @After
    public void after() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void membersDefinedViaIps() {
        test("127.0.0.1:5701", "127.0.0.1:5702", "127.0.0.1:5703");
    }

    @Test
    public void membersDefinedViaHostname() {
        test("localhost:5701", "localhost:5702", "localhost:5703");
    }

    private void test(String... members) {
        //start 3 member cluster
        final HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(getConfig(members));
        final HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(getConfig(members));
        final HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(getConfig(members));
        assertClusterSize(3, hz1, hz2, hz3);

        CapturingCollector collector = new CapturingCollector();
        getNodeEngineImpl(hz1).getMetricsRegistry().collect(collector);

        verifyMetricVale(collector, metricDescriptor(TCP_METRIC_ENDPOINT_MANAGER_COUNT), 2, 2);
        verifyMetricVale(collector, metricDescriptor(TCP_METRIC_ENDPOINT_MANAGER_ACTIVE_COUNT), 2, 3);
        verifyNoOfCollectedMetrics(collector, TCP_DISCRIMINATOR_ENDPOINT, 2, 3); //https://github.com/hazelcast/hazelcast/issues/18877
        verifyNoOfCollectedMetrics(collector, TCP_DISCRIMINATOR_BINDADDRESS, 2, 3); //https://github.com/hazelcast/hazelcast/issues/18877
    }

    private void verifyMetricVale(
            CapturingCollector collector,
            MetricDescriptor expectedDescriptor,
            long expectedLow, long expectedHigh
    ) {
        CapturingCollector.Capture capture = collector.captures().get(expectedDescriptor);
        assertNotNull(capture);
        assertEquals(expectedDescriptor.toString(), 1, capture.hits());
        long actual = (long) capture.singleCapturedValue();
        assertTrue(expectedLow <= actual && actual <= expectedHigh);
    }

    private void verifyNoOfCollectedMetrics(
            CapturingCollector collector,
            String discriminator,
            long expectedLow,
            long expectedHigh
    ) {
        Map<MetricDescriptor, CapturingCollector.Capture> captures = collector.captures().entrySet().stream()
                .filter(e -> Objects.equals(e.getKey().prefix(), TCP_PREFIX_CONNECTION))
                .filter(e -> Objects.equals(e.getKey().discriminator(), discriminator))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        int actual = captures.size();
        assertTrue(
                String.format("Actual value of %d for %s not in range [%d, %d]",
                        actual, discriminator, expectedLow, expectedHigh),
                expectedLow <= actual && actual <= expectedHigh);
    }

    private MetricDescriptor metricDescriptor(String metric) {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get()
                .withPrefix(TCP_PREFIX_CONNECTION)
                .withMetric(metric)
                .withUnit(COUNT);
    }

    private Config getConfig(String... members) {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        for (String member : members) {
            join.getTcpIpConfig().addMember(member);
        }
        return config;
    }

}
