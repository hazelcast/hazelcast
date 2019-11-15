/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.managementcenter;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.impl.MetricsCompressor;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ReadMetricsTest extends HazelcastTestSupport {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void when_readMetricsAsync() {
        Config conf = getConfig();
        conf.getMetricsConfig()
            .setEnabled(true)
            .setCollectionIntervalSeconds(1);

        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(conf);
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());
        Member member = instance.getCluster().getLocalMember();

        AtomicLong nextSequence = new AtomicLong();
        assertTrueEventually(() -> {
            MetricsResultSet result = client.getManagementCenterService()
                    .readMetricsAsync(member, nextSequence.get()).get();
            nextSequence.set(result.nextSequence());
            // call should not return empty result - it should wait until a result is available
            assertFalse("empty result", result.collections().isEmpty());

            boolean operationMetricFound = false;
            byte[] blob = result.collections().get(0).getValue();
            MetricDescriptor expectedDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER.get()
                                                                             .withPrefix("operation")
                                                                             .withMetric("queueSize")
                                                                             .withUnit(COUNT);
            MetricKeyConsumer metricConsumer = new MetricKeyConsumer(expectedDescriptor);
            MetricsCompressor.extractMetrics(blob, metricConsumer);
            assertTrue(metricConsumer.operationMetricFound);
        });
    }

    @Test
    public void when_invalidUUID() throws ExecutionException, InterruptedException {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(getConfig());
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());
        Address addr = instance.getCluster().getLocalMember().getAddress();
        MemberVersion ver = instance.getCluster().getLocalMember().getVersion();
        MemberImpl member = new MemberImpl(addr, ver, false, UuidUtil.newUnsecureUUID());

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.getManagementCenterService().readMetricsAsync(member, 0).get();
    }

    @Test
    public void when_metricsDisabled() throws ExecutionException, InterruptedException {
        Config cfg = getConfig();
        cfg.getMetricsConfig().setEnabled(false);
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance(cfg);
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(hazelcastFactory.newHazelcastClient());

        exception.expectCause(Matchers.instanceOf(IllegalArgumentException.class));
        client.getManagementCenterService()
                .readMetricsAsync(instance.getCluster().getLocalMember(), 0).get();
    }

    private static class MetricKeyConsumer implements MetricConsumer {

        private final MetricDescriptor expectedDescriptor;
        private boolean operationMetricFound;

        private MetricKeyConsumer(MetricDescriptor expectedDescriptor) {
            this.expectedDescriptor = expectedDescriptor;
        }

        @Override
        public void consumeLong(MetricDescriptor descriptor, long value) {
            operationMetricFound |= descriptor.equals(expectedDescriptor);
        }

        @Override
        public void consumeDouble(MetricDescriptor descriptor, double value) {
            operationMetricFound |= descriptor.equals(expectedDescriptor);
        }
    }
}
