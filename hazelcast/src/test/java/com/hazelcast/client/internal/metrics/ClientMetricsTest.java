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

package com.hazelcast.client.internal.metrics;

import com.hazelcast.client.Client;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.statistics.ClientStatistics;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.impl.CapturingCollector;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMetricsTest extends HazelcastTestSupport {

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testLongClientMetricIsMergedIntoMemberMetrics() {
        HazelcastInstance memberInstance = givenMemberWithTwoClients();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(memberInstance).getMetricsRegistry();

        // randomly chosen long client metric, we just check that
        // we have client-side metrics merged into the member metrics
        assertClientMetricsObservedEventually(memberInstance, metricsRegistry, "gc", "minorCount", COUNT);
    }

    @Test
    public void testDoubleClientMetricIsMergedIntoMemberMetrics() {
        HazelcastInstance memberInstance = givenMemberWithTwoClients();
        MetricsRegistry metricsRegistry = getNodeEngineImpl(memberInstance).getMetricsRegistry();

        // randomly chosen double client metric, we just check that
        // we have client-side metrics merged into the member metrics
        assertClientMetricsObservedEventually(memberInstance, metricsRegistry, "os", "systemLoadAverage", null);
    }

    private HazelcastInstance givenMemberWithTwoClients() {
        Config conf = getConfig();
        conf.getMetricsConfig()
            .setEnabled(true)
            .setCollectionFrequencySeconds(1);

        HazelcastInstance memberInstance = hazelcastFactory.newHazelcastInstance(conf);
        hazelcastFactory.newHazelcastClient();
        hazelcastFactory.newHazelcastClient();
        return memberInstance;
    }

    private void assertClientMetricsObservedEventually(HazelcastInstance memberInstance, MetricsRegistry metricsRegistry,
                                                       String prefix, String metric, ProbeUnit unit) {
        assertTrueEventually(() -> {
            CapturingCollector collector = new CapturingCollector();
            metricsRegistry.collect(collector);

            ClientEngine clientEngine = getNode(memberInstance).getClientEngine();
            Collection<Client> clients = clientEngine.getClients();

            assertEquals(2, clients.size());

            for (Client client : clients) {
                UUID clientUuid = client.getUuid();
                ClientStatistics clientStatistics = clientEngine.getClientStatistics().get(clientUuid);
                assertNotNull(clientStatistics);
                long timestamp = clientStatistics.timestamp();

                MetricDescriptor expectedDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                        .get()
                        .withPrefix(prefix)
                        .withMetric(metric)
                        .withUnit(unit)
                        .withTag("client", clientUuid.toString())
                        .withTag("clientname", client.getName())
                        .withTag("timestamp", Long.toString(timestamp))
                        .withExcludedTargets(asList(MetricTarget.values()))
                        .withIncludedTarget(MANAGEMENT_CENTER);

                assertContains(collector.captures().keySet(), expectedDescriptor);
            }
        });
    }

}
