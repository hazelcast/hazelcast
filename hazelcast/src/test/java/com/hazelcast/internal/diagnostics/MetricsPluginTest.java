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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MetricsPluginTest extends AbstractDiagnosticsPluginTest {

    private MetricsPlugin plugin;
    private MetricsRegistry metricsRegistry;

    @Before
    public void setup() {
        Config config = new Config()
                .setProperty(Diagnostics.ENABLED.getName(), "true")
                .setProperty(MetricsPlugin.PERIOD_SECONDS.getName(), "1");
        HazelcastInstance hz = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(hz);
        metricsRegistry = nodeEngineImpl.getMetricsRegistry();
        plugin = new MetricsPlugin(nodeEngineImpl);
        plugin.onStart();
    }

    @Test
    public void testGetPeriodMillis() {
        long periodMillis = plugin.getPeriodMillis();
        assertEquals(SECONDS.toMillis(1), periodMillis);
    }

    @Test
    public void testRunWithProblematicProbe() {
        metricsRegistry.registerStaticProbe(this, "broken", MANDATORY, (LongProbeFunction) source -> {
            throw new RuntimeException("error");
        });

        plugin.run(logWriter);
        assertContains("[metric=broken,excludedTargets={}]=java.lang.RuntimeException:error");
    }

    @Test
    public void testRun() {
        plugin.run(logWriter);

        // we just test a few to make sure the metrics are written
        assertContains("[unit=count,metric=client.endpoint.count,excludedTargets={}]=0");
        assertContains("[unit=count,metric=operation.responseQueueSize,excludedTargets={}]=0");
    }

    @Test
    public void testExclusion() {
        metricsRegistry.registerStaticMetrics(new ExclusionProbeSource(), "test");

        plugin.run(logWriter);

        assertContains("[unit=count,metric=test.notExcludedLong,excludedTargets={}]=1");
        assertNotContains("[unit=count,metric=test.excludedLong,excludedTargets={}]=1");
        assertContains("[unit=count,metric=test.notExcludedDouble,excludedTargets={}]=1.5");
        assertNotContains("[unit=count,metric=test.excludedDouble,excludedTargets={}]=2.5");
    }

    private static class ExclusionProbeSource {
        @Probe
        private long notExcludedLong = 1;

        @Probe(excludedTargets = DIAGNOSTICS)
        private long excludedLong = 2;

        @Probe
        private double notExcludedDouble = 1.5D;

        @Probe(excludedTargets = DIAGNOSTICS)
        private double excludedDouble = 2.5D;
    }

}
