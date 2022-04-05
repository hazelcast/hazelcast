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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class DynamicMetricsCollectionTest extends HazelcastTestSupport {

    @Test
    public void testExtractingFromObject() {
        SourceObject source = new SourceObject();
        source.longField = 42;
        source.doubleField = 42.42D;

        CapturingCollector collector = new CapturingCollector();
        MetricsRegistry registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        registry.registerDynamicMetricsProvider((descriptor, context) -> {
            context.collect(descriptor.withPrefix("test"), source);
        });
        registry.collect(collector);

        assertEquals(42L, collector.captures()
                                   .get(registry.newMetricDescriptor()
                                                .withPrefix("test")
                                                .withMetric("longField")
                                                .withUnit(COUNT)).singleCapturedValue());
        assertEquals(42.42D, collector.captures()
                                      .get(registry.newMetricDescriptor()
                                                   .withPrefix("test")
                                                   .withMetric("doubleField")
                                                   .withUnit(COUNT)).singleCapturedValue());
        assertEquals(43L, collector.captures()
                                   .get(registry.newMetricDescriptor()
                                                .withPrefix("test")
                                                .withMetric("longMethod")
                                                .withUnit(COUNT)).singleCapturedValue());
        assertEquals(43.52D, collector.captures()
                                      .get(registry.newMetricDescriptor()
                                                   .withPrefix("test")
                                                   .withMetric("doubleMethod")
                                                   .withUnit(COUNT)).singleCapturedValue());
    }

    @Test
    public void testExtractingFromObjectProveLevelFilters() {
        SourceObject source = new SourceObject();
        source.longField = 42;
        source.doubleField = 42.42D;

        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry registry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        registry.registerDynamicMetricsProvider((descriptor, context) -> context.collect(descriptor.withPrefix("test"), source));
        registry.collect(collectorMock);

        verify(collectorMock, never()).collectLong(registry.newMetricDescriptor()
                                                           .withPrefix("test")
                                                           .withMetric("longField"), 42);
        verify(collectorMock, never()).collectDouble(registry.newMetricDescriptor()
                                                             .withPrefix("test")
                                                             .withMetric("doubleField"), 42.42D);
        verify(collectorMock, never()).collectLong(registry.newMetricDescriptor()
                                                           .withPrefix("test")
                                                           .withMetric("longMethod"), 43);
        verify(collectorMock, never()).collectDouble(registry.newMetricDescriptor()
                                                             .withPrefix("test")
                                                             .withMetric("doubleMethod"), 43.52D);
    }

    @Test
    public void testDirectLong() {
        CapturingCollector capturingCollector = new CapturingCollector();
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context.collect(descriptor.withPrefix("test"), "someMetric", INFO, BYTES, 42));
        metricsRegistry.collect(capturingCollector);

        MetricDescriptor expectedDescriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix("test")
                .withUnit(BYTES)
                .withMetric("someMetric");

        Number number = capturingCollector.captures().get(expectedDescriptor).singleCapturedValue();
        assertInstanceOf(Long.class, number);
        assertEquals(42, number.longValue());
    }

    @Test
    public void testDirectLongProveLevelFilters() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context.collect(descriptor.withPrefix("test"), "someMetric", INFO, BYTES, 42));
        metricsRegistry.collect(collectorMock);

        MetricDescriptor expectedDescriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix("test")
                .withUnit(BYTES)
                .withMetric("someMetric");
        verify(collectorMock, never()).collectLong(expectedDescriptor, 42);
    }

    @Test
    public void testDirectDouble() {
        CapturingCollector capturingCollector = new CapturingCollector();
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context.collect(descriptor.withPrefix("test"), "someMetric", INFO, BYTES, 42.42D));
        metricsRegistry.collect(capturingCollector);

        MetricDescriptor expectedDescriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix("test")
                .withUnit(BYTES)
                .withMetric("someMetric");
        Number number = capturingCollector.captures().get(expectedDescriptor).singleCapturedValue();
        assertInstanceOf(Double.class, number);
        assertEquals(42.42D, number.doubleValue(), 10E-6);
    }

    @Test
    public void testDirectDoubleProveLevelFilters() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider(
                (descriptor, context) -> context.collect(descriptor.withPrefix("test"), "someMetric", INFO, BYTES, 42.42D));
        metricsRegistry.collect(collectorMock);

        MetricDescriptor expectedDescriptor = metricsRegistry
                .newMetricDescriptor()
                .withPrefix("test")
                .withUnit(BYTES)
                .withMetric("someMetric");
        verify(collectorMock, never()).collectDouble(expectedDescriptor, 42.42D);
    }

    @RequireAssertEnabled
    @Test
    public void testDynamicProviderExceptionsAreNotPropagated() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            throw new RuntimeException("Intentionally failing metrics collection");

        });

        // we just expect there is no exception apart from AssertionError,
        // which is for testing only
        Assert.assertThrows(AssertionError.class, () -> metricsRegistry.collect(collectorMock));
    }

    private static class SourceObject {
        @Probe(name = "longField")
        private long longField;
        @Probe(name = "doubleField")
        private double doubleField;

        @Probe(name = "longMethod")
        private long longMethod() {
            return longField + 1;
        }

        @Probe(name = "doubleMethod")
        private double doubleMethod() {
            return doubleField + 1.1D;
        }
    }
}
