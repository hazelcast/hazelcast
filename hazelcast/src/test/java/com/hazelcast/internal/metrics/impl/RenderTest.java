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
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.ExpectedRuntimeException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class RenderTest {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), ProbeLevel.INFO);
    }

    private void registerLongMetric(String name, final int value) {
        metricsRegistry.registerStaticProbe(this, name, ProbeLevel.INFO,
                (LongProbeFunction<RenderTest>) source -> value);
    }


    private void registerDoubleMetric(String name, final int value) {
        metricsRegistry.registerStaticProbe(this, name, ProbeLevel.INFO,
                (DoubleProbeFunction<RenderTest>) source -> value);
    }

    @Test(expected = NullPointerException.class)
    public void whenCalledWithNullRenderer() {
        metricsRegistry.collect(null);
    }

    @Test
    public void whenLongProbeFunctions() {
        MetricsCollector renderer = mock(MetricsCollector.class);

        registerLongMetric("foo", 10);
        registerLongMetric("bar", 20);

        metricsRegistry.collect(renderer);

        verify(renderer).collectLong(metricDescriptor("foo"), 10);
        verify(renderer).collectLong(metricDescriptor("bar"), 20);
        verifyNoMoreInteractions(renderer);
    }

    private MetricDescriptor metricDescriptor(String metric) {
        return DEFAULT_DESCRIPTOR_SUPPLIER.get()
                                          .withMetric(metric);
    }

    @Test
    public void whenDoubleProbeFunctions() {
        MetricsCollector renderer = mock(MetricsCollector.class);

        registerDoubleMetric("foo", 10);
        registerDoubleMetric("bar", 20);

        metricsRegistry.collect(renderer);

        verify(renderer).collectDouble(metricDescriptor("foo"), 10);
        verify(renderer).collectDouble(metricDescriptor("bar"), 20);
        verifyNoMoreInteractions(renderer);
    }

    @Test
    public void whenException() {
        MetricsCollector renderer = mock(MetricsCollector.class);

        final ExpectedRuntimeException ex = new ExpectedRuntimeException();

        metricsRegistry.registerStaticProbe(this, "foo", ProbeLevel.MANDATORY,
                (LongProbeFunction<RenderTest>) source -> {
                    throw ex;
                });

        metricsRegistry.collect(renderer);

        verify(renderer).collectException(metricDescriptor("foo"), ex);
        verifyNoMoreInteractions(renderer);
    }
}
