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

package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricTagger;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricTaggerImplTest {

    @Probe
    private int probe1 = 1;

    @Probe(name = "secondProbe", level = ProbeLevel.MANDATORY, unit = ProbeUnit.BYTES)
    private int probe2 = 2;

    @Probe(level = ProbeLevel.DEBUG)
    private int probe3 = 3;

    @Test
    public void test_scanAndRegister() {
        testScanAndRegister(null);
    }

    @Test
    public void test_scanAndRegisterWithPrefix() {
        testScanAndRegister("testProbe");
    }

    @Test
    public void test_register() {
        testRegister(null);
    }

    @Test
    public void test_registerWithPrefix() {
        testRegister("test.prefix");
    }

    @Test
    public void testMetricId_withPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("prefix[idValue].metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withPrefixWithoutId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withMetricTag("metricValue");

        assertEquals("prefix.metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withoutPrefixWithId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idValue].metricValue", tagger.metricId());
    }

    @Test
    public void testMetricId_withoutPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withMetricTag("metricValue");

        assertEquals("metricValue", tagger.metricId());
    }

    @Test
    public void testMetricName_withPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idTag=idValue,metric=prefix.metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withPrefixWithoutId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, "prefix")
                .withMetricTag("metricValue");

        assertEquals("[metric=prefix.metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixWithId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withIdTag("idTag", "idValue")
                .withMetricTag("metricValue");

        assertEquals("[idTag=idValue,metric=metricValue]", tagger.metricName());
    }

    @Test
    public void testMetricName_withoutPrefixAndId() {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTaggerImpl tagger = new MetricTaggerImpl(registry, null)
                .withMetricTag("metricValue");

        assertEquals("[metric=metricValue]", tagger.metricName());
    }

    private void testScanAndRegister(String prefix) {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTagger builder = prefix != null ? registry.newMetricTagger(prefix) : registry.newMetricTagger();
        builder
                .withTag("tag1", "value1")
                .registerStaticMetrics(this);

        assertProbes(registry, prefix);
    }

    private void testRegister(String prefix) {
        MetricsRegistryImpl registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
        MetricTagger builder = prefix != null ? registry.newMetricTagger(prefix) : registry.newMetricTagger();
        builder = builder.withTag("tag1", "value1");
        builder.registerStaticProbe(this, "probe1", ProbeLevel.INFO, ProbeUnit.COUNT,
                (LongProbeFunction<MetricTaggerImplTest>) source -> source.probe1);
        builder.registerStaticProbe(this, "secondProbe", ProbeLevel.INFO, ProbeUnit.BYTES,
                (LongProbeFunction<MetricTaggerImplTest>) source -> source.probe2);

        assertProbes(registry, prefix);
    }

    private void assertProbes(MetricsRegistryImpl registry, String prefix) {
        prefix = prefix != null ? prefix + "." : "";
        final String p1Name = "[tag1=value1,unit=count,metric=" + prefix + "probe1]";
        final String p2Name = "[tag1=value1,unit=bytes,metric=" + prefix + "secondProbe]";
        assertEquals(new HashSet<>(asList(p1Name, p2Name)), registry.getNames());

        registry.collect(new MetricsCollector() {
            @Override
            public void collectLong(String name, long value) {
                if (p1Name.equals(name)) {
                    assertEquals(probe1, value);
                } else if (p2Name.equals(name)) {
                    assertEquals(probe2, value);
                } else {
                    fail("Unknown metric: " + name);
                }
            }

            @Override
            public void collectDouble(String name, double value) {
                fail("Unknown metric: " + name);
            }

            @Override
            public void collectException(String name, Exception e) {
                throw new RuntimeException(e);
            }

            @Override
            public void collectNoValue(String name) {
                fail("Unknown metric: " + name);
            }
        });
    }
}
