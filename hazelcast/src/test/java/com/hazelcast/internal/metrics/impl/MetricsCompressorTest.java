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

import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.function.Supplier;

import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsCompressorTest {

    private final DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
    private final Supplier<? extends MetricDescriptor> supplierSpy = Mockito.spy(supplier);
    private final MetricsCompressor compressor = new MetricsCompressor();

    @Test
    public void testSingleLongMetric() {
        MetricDescriptor originalMetric = supplier.get()
                                                  .withPrefix("prefix")
                                                  .withMetric("metricName")
                                                  .withDiscriminator("ds", "dsName1")
                                                  .withUnit(COUNT)
                                                  .withTag("tag0", "tag0Value");
        compressor.addLong(originalMetric, 42L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(originalMetric, 42L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, only()).get();
    }

    @Test
    public void testSingleDoubleMetric() {
        MetricDescriptor originalMetric = supplier.get()
                                                  .withPrefix("prefix")
                                                  .withMetric("metricName")
                                                  .withDiscriminator("ds", "dsName1")
                                                  .withUnit(COUNT)
                                                  .withTag("tag0", "tag0Value");
        compressor.addDouble(originalMetric, 42.42D);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeDouble(originalMetric, 42.42D);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, only()).get();
    }

    @Test
    public void testSingleMetricWithoutPrefix() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor originalMetric = supplier.get()
                                                  .withMetric("metricName")
                                                  .withDiscriminator("ds", "dsName1")
                                                  .withUnit(COUNT)
                                                  .withTag("tag0", "tag0Value");
        compressor.addLong(originalMetric, 42L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(originalMetric, 42L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, only()).get();
    }

    @Test
    public void testSingleMetricWithoutUnit() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor originalMetric = supplier.get()
                                                  .withMetric("metricName")
                                                  .withMetric("metricName")
                                                  .withDiscriminator("ds", "dsName1")
                                                  .withTag("tag0", "tag0Value");
        compressor.addLong(originalMetric, 42L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(originalMetric, 42L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, only()).get();
    }

    @Test
    public void testTwoMetrics_withDelta() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withDiscriminator("ds", "dsName1")
                                           .withUnit(COUNT)
                                           .withTag("tag0", "tag0Value");
        MetricDescriptor metric2 = metric1.copy()
                                          .withMetric("metricName2");
        compressor.addLong(metric1, 42L);
        compressor.addLong(metric2, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(metric1, 42L);
        verify(metricConsumerMock).consumeLong(metric2, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(2)).get();
    }

    @Test
    public void testTwoMetrics_fullDifferent() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withDiscriminator("ds", "dsName1")
                                           .withUnit(COUNT)
                                           .withTag("tag0", "tag0Value");
        MetricDescriptor metric2 = supplier.get()
                                           .withPrefix("anotherPrefix")
                                           .withMetric("anotherMetricName")
                                           .withDiscriminator("anotherDs", "anotherDsName1")
                                           .withUnit(PERCENT)
                                           .withTag("anotherTag0", "anotherTag0Value");
        compressor.addLong(metric1, 42L);
        compressor.addLong(metric2, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(metric1, 42L);
        verify(metricConsumerMock).consumeLong(metric2, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(2)).get();
    }

    @Test
    public void testTwoMetrics_secondWithoutTags() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withDiscriminator("ds", "dsName1")
                                           .withUnit(COUNT);
        MetricDescriptor metric2 = metric1.copy()
                                          .withMetric("metricName2");
        metric1.withTag("tag0", "tag0Value");

        compressor.addLong(metric1, 42L);
        compressor.addLong(metric2, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(metric1, 42L);
        verify(metricConsumerMock).consumeLong(metric2, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(2)).get();
    }

    @Test
    public void testTwoMetrics_sameExcludedTargets() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withExcludedTarget(JMX)
                                           .withExcludedTarget(MANAGEMENT_CENTER);
        MetricDescriptor metric2 = metric1.copy();

        compressor.addLong(metric1, 42L);
        compressor.addLong(metric2, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(metric1, 42L);
        verify(metricConsumerMock).consumeLong(metric2, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(2)).get();
    }

    @Test
    public void testTwoMetrics_differentExcludedTargets() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withExcludedTarget(JMX);
        MetricDescriptor metric2 = metric1.copy()
                                          .withExcludedTarget(MANAGEMENT_CENTER);

        compressor.addLong(metric1, 42L);
        compressor.addLong(metric2, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        verify(metricConsumerMock).consumeLong(metric1, 42L);
        verify(metricConsumerMock).consumeLong(metric2, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(2)).get();
    }
}
