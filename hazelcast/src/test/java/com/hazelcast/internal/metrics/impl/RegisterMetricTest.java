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

import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RegisterMetricTest extends HazelcastTestSupport {

    public static final int IGNORED = 42;
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test(expected = NullPointerException.class)
    public void whenNamePrefixNull() {
        metricsRegistry.registerStaticMetrics(new SomeField(), null);
    }

    @Test(expected = NullPointerException.class)
    public void whenObjectNull() {
        metricsRegistry.registerStaticMetrics((Object) null, "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUnrecognizedField() {
        metricsRegistry.registerStaticMetrics(new SomeUnrecognizedField(), "bar");
    }

    @Test
    public void whenNoGauges_thenIgnore() {
        metricsRegistry.registerStaticMetrics(new LinkedList(), "bar");

        for (String name : metricsRegistry.getNames()) {
            assertFalse(name.startsWith("bar"));
        }
    }

    @Test
    public void testRegisterStaticMetrics() {
        MultiFieldAndMethod source = new MultiFieldAndMethod();
        metricsRegistry.registerStaticMetrics(metricsRegistry.newMetricDescriptor().withPrefix("test"), source);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[unit=count,metric=test.method1]");
        assertContains(metricNames, "[unit=count,metric=test.method2]");
        assertContains(metricNames, "[unit=count,metric=test.field1]");
        assertContains(metricNames, "[unit=count,metric=test.field2]");
    }

    // long functions

    @Test
    public void testRegisterStaticProbeLong_withTagger() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), metricsRegistry.newMetricDescriptor().withPrefix("test"),
                "someMetric", INFO, BYTES, (LongProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[unit=bytes,metric=test.someMetric]");
    }

    @Test
    public void testRegisterStaticProbeLong_withoutTagger() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), "someMetric", INFO, BYTES,
                (LongProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[unit=bytes,metric=someMetric]");
    }

    @Test
    public void testRegisterStaticProbeLong_withoutTagger_withoutUnit() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), "someMetric", INFO,
                (LongProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[metric=someMetric]");
    }

    // double functions

    @Test
    public void testRegisterStaticProbeDouble_withTagger() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), metricsRegistry.newMetricDescriptor().withPrefix("test"),
                "someMetric", INFO, BYTES, (DoubleProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[unit=bytes,metric=test.someMetric]");
    }

    @Test
    public void testRegisterStaticProbeDouble_withoutTagger() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), "someMetric", INFO, BYTES,
                (DoubleProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[unit=bytes,metric=someMetric]");
    }

    @Test
    public void testRegisterStaticProbeDouble_withoutTagger_withoutUnit() {
        metricsRegistry.registerStaticProbe(new MultiFieldAndMethod(), "someMetric", INFO,
                (DoubleProbeFunction<MultiFieldAndMethod>) source -> IGNORED);
        Set<String> metricNames = metricsRegistry.getNames();

        assertContains(metricNames, "[metric=someMetric]");
    }

    public class SomeField {
        @Probe(name = "field")
        long field;
    }

    public class SomeUnrecognizedField {
        @Probe(name = "field")
        OutputStream field;
    }

    public class MultiFieldAndMethod {
        @Probe(name = "field1")
        long field1;
        @Probe(name = "field2")
        double field2;

        @Probe(name = "method1")
        int method1() {
            return 1;
        }

        @Probe(name = "method2")
        double method2() {
            return 2;
        }
    }
}
