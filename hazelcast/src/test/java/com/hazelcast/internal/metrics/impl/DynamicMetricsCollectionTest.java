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

import com.hazelcast.internal.metrics.MetricTagger;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static java.util.Collections.emptySet;
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

        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, source);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock).collectLong("[unit=count,metric=test.longField]", 42, emptySet());
        verify(collectorMock).collectDouble("[unit=count,metric=test.doubleField]", 42.42D, emptySet());
        verify(collectorMock).collectLong("[unit=count,metric=test.longMethod]", 43, emptySet());
        verify(collectorMock).collectDouble("[unit=count,metric=test.doubleMethod]", 43.52D, emptySet());
    }

    @Test
    public void testExtractingFromObjectProveLevelFilters() {
        SourceObject source = new SourceObject();
        source.longField = 42;
        source.doubleField = 42.42D;

        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, source);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock, never()).collectLong("[unit=count,metric=test.longField]", 42, emptySet());
        verify(collectorMock, never()).collectDouble("[unit=count,metric=test.doubleField]", 42.42D, emptySet());
        verify(collectorMock, never()).collectLong("[unit=count,metric=test.longMethod]", 43, emptySet());
        verify(collectorMock, never()).collectDouble("[unit=count,metric=test.doubleMethod]", 43.52D, emptySet());
    }

    @Test
    public void testDirectLong() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, "someMetric", INFO, BYTES, 42);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock).collectLong("[unit=bytes,metric=test.someMetric]", 42, emptySet());
    }

    @Test
    public void testDirectLongProveLevelFilters() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, "someMetric", INFO, BYTES, 42);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock, never()).collectLong("[unit=bytes,metric=test.someMetric]", 42, emptySet());
    }

    @Test
    public void testDirectDouble() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, "someMetric", INFO, BYTES, 42.42D);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock).collectDouble("[unit=bytes,metric=test.someMetric]", 42.42D, emptySet());
    }

    @Test
    public void testDirectDoubleProveLevelFilters() {
        MetricsCollector collectorMock = mock(MetricsCollector.class);
        MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), MANDATORY);
        metricsRegistry.registerDynamicMetricsProvider((taggerSupplier, context) -> {
            MetricTagger tagger = taggerSupplier.getMetricTagger("test");
            context.collect(tagger, "someMetric", INFO, BYTES, 42.42D);
        });
        metricsRegistry.collect(collectorMock);

        verify(collectorMock, never()).collectDouble("[unit=bytes,metric=test.someMetric]", 42.42D, emptySet());
    }

    private static class SourceObject {
        @Probe
        private long longField;
        @Probe
        private double doubleField;

        @Probe
        private long longMethod() {
            return longField + 1;
        }

        @Probe
        private double doubleMethod() {
            return doubleField + 1.1D;
        }
    }
}
