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

import com.hazelcast.internal.metrics.ExcludedMetricTargets;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static com.hazelcast.internal.metrics.MetricTarget.ALL_TARGETS;
import static com.hazelcast.internal.metrics.MetricTarget.ALL_TARGETS_BUT_DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JET_JOB;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.MetricTarget.NONE_OF;
import static com.hazelcast.internal.metrics.MetricTarget.asSet;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsTargetExclusionTest {
    private MetricsRegistryImpl metricsRegistry;
    private CapturingCollector collector = new CapturingCollector();

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), DEBUG);
    }

    @Test
    public void testStaticMetrics() {
        metricsRegistry.registerStaticMetrics(new SomeObject(), "test");
        metricsRegistry.collect(collector);

        // fields
        verifyCollected("test", "nonDebugLongField", NONE_OF);
        verifyCollected("test", "debugLongField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongFieldNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleField", NONE_OF);
        verifyCollected("test", "debugDoubleField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleFieldNoDiag", ALL_TARGETS);

        // methods
        verifyCollected("test", "nonDebugLongMethod", NONE_OF);
        verifyCollected("test", "debugLongMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongMethodNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleMethod", NONE_OF);
        verifyCollected("test", "debugDoubleMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleMethodNoDiag", ALL_TARGETS);
    }

    @Test
    public void testStaticMetricsWithClassExclusion() {
        metricsRegistry.registerStaticMetrics(new SomeObjectWithClassExclusion(), "test");
        metricsRegistry.collect(collector);

        // fields
        verifyCollected("test", "nonDebugLongField", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugLongField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongFieldNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleField", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugDoubleField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleFieldNoDiag", ALL_TARGETS);

        // methods
        verifyCollected("test", "nonDebugLongMethod", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugLongMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongMethodNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleMethod", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugDoubleMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleMethodNoDiag", ALL_TARGETS);
    }

    @Test
    public void testDynamicMetrics() {
        metricsRegistry.registerDynamicMetricsProvider(((descriptor, context) -> {
            // values
            MetricDescriptor descriptorValues = descriptor.copy().withPrefix("testValues");
            context.collect(descriptorValues.copy().withMetric("noLevelLong").withUnit(COUNT), 42L);
            context.collect(descriptorValues.copy(), "nonDebugLong", INFO, COUNT, 42L);
            context.collect(descriptorValues.copy().withExcludedTargets(ALL_TARGETS_BUT_DIAGNOSTICS), "debugLong", DEBUG, COUNT, 42L);
            context.collect(descriptorValues.copy().withExcludedTargets(ALL_TARGETS), "debugLongNoDiag", DEBUG, COUNT, 42L);
            context.collect(descriptorValues.copy().withMetric("noLevelDouble").withUnit(COUNT), 42L);
            context.collect(descriptorValues.copy(), "nonDebugDouble", INFO, COUNT, 42.42D);
            context.collect(descriptorValues.copy().withExcludedTargets(ALL_TARGETS_BUT_DIAGNOSTICS), "debugDouble", DEBUG, COUNT, 42.42D);
            context.collect(descriptorValues.copy().withExcludedTargets(ALL_TARGETS), "debugDoubleNoDiag", DEBUG, COUNT, 42.42D);

            // object
            context.collect(descriptor.withPrefix("test"), new SomeObject());
        }));

        metricsRegistry.collect(collector);

        // values
        verifyCollected("testValues", "noLevelLong", NONE_OF);
        verifyCollected("testValues", "nonDebugLong", NONE_OF);
        verifyCollected("testValues", "debugLong", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("testValues", "debugLongNoDiag", ALL_TARGETS);
        verifyCollected("testValues", "noLevelDouble", NONE_OF);
        verifyCollected("testValues", "nonDebugDouble", NONE_OF);
        verifyCollected("testValues", "debugDouble", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("testValues", "debugDoubleNoDiag", ALL_TARGETS);

        // object - fields
        verifyCollected("test", "nonDebugLongField", NONE_OF);
        verifyCollected("test", "debugLongField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongFieldNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleField", NONE_OF);
        verifyCollected("test", "debugDoubleField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleFieldNoDiag", ALL_TARGETS);

        // object - methods
        verifyCollected("test", "nonDebugLongMethod", NONE_OF);
        verifyCollected("test", "debugLongMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongMethodNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleMethod", NONE_OF);
        verifyCollected("test", "debugDoubleMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleMethodNoDiag", ALL_TARGETS);
    }

    @Test
    public void testDynamicMetricsWithClassExclusion() {
        metricsRegistry.registerDynamicMetricsProvider(((descriptor, context) -> {
            // object
            context.collect(descriptor.withPrefix("test"), new SomeObjectWithClassExclusion());
        }));

        metricsRegistry.collect(collector);

        // object - fields
        verifyCollected("test", "nonDebugLongField", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugLongField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongFieldNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleField", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugDoubleField", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleFieldNoDiag", ALL_TARGETS);

        // object - methods
        verifyCollected("test", "nonDebugLongMethod", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugLongMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugLongMethodNoDiag", ALL_TARGETS);
        verifyCollected("test", "nonDebugDoubleMethod", asSet(MANAGEMENT_CENTER));
        verifyCollected("test", "debugDoubleMethod", ALL_TARGETS_BUT_DIAGNOSTICS);
        verifyCollected("test", "debugDoubleMethodNoDiag", ALL_TARGETS);
    }

    private void verifyCollected(String prefix, String metricName, Collection<MetricTarget> excludedTargets) {
        MetricDescriptor descriptor = DEFAULT_DESCRIPTOR_SUPPLIER
            .get()
            .withPrefix(prefix)
            .withMetric(metricName)
            .withUnit(COUNT)
            .withExcludedTargets(excludedTargets);

        assertTrue(collector.isCaptured(descriptor));
    }

    private static class SomeObject {

        @Probe(name = "nonDebugLongField")
        private long nonDebugLongField;
        @Probe(name = "debugLongField", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX})
        private long debugLongField;
        @Probe(name = "debugLongFieldNoDiag", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX, DIAGNOSTICS})
        private long debugLongFieldNoDiag;
        @Probe(name = "nonDebugDoubleField")
        private double nonDebugDoubleField;
        @Probe(name = "debugDoubleField", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX})
        private double debugDoubleField;
        @Probe(name = "debugDoubleFieldNoDiag", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX, DIAGNOSTICS})
        private long debugDoubleFieldNoDiag;

        @Probe(name = "nonDebugLongMethod")
        private long nonDebugLongMethod() {
            return 0;
        }

        @Probe(name = "debugLongMethod", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX})
        private long debugLongMethod() {
            return 0;
        }

        @Probe(name = "debugLongMethodNoDiag", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX, DIAGNOSTICS})
        private long debugLongMethodNoDiag() {
            return 0;
        }

        @Probe(name = "nonDebugDoubleMethod")
        private double nonDebugDoubleMethod() {
            return 0;
        }

        @Probe(name = "debugDoubleMethod", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX})
        private double debugDoubleMethod() {
            return 0;
        }

        @Probe(name = "debugDoubleMethodNoDiag", level = DEBUG, excludedTargets = {MANAGEMENT_CENTER, JET_JOB, JMX, DIAGNOSTICS})
        private long debugDoubleMethodNoDiag() {
            return 0;
        }
    }

    @ExcludedMetricTargets(MANAGEMENT_CENTER)
    private static class SomeObjectWithClassExclusion extends SomeObject {
    }
}
