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

import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeUnit;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static com.hazelcast.internal.metrics.MetricTarget.JMX;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.ProbeUnit.PERCENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsCompressorTest {

    private static final String LONG_NAME = Stream.generate(() -> "a")
                                                 .limit(MetricsDictionary.MAX_WORD_LENGTH + 1)
                                                 .collect(Collectors.joining());

    private final DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
    private final Supplier<? extends MetricDescriptor> supplierSpy = spy(supplier);
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

    @Test
    public void testModifyingExtractedDoesntImpactExtraction() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        MetricDescriptor metric1 = supplier.get()
                                           .withPrefix("prefix")
                                           .withMetric("metricName")
                                           .withDiscriminator("name", "objName")
                                           .withUnit(PERCENT)
                                           .withTag("tag0", "tag0Value")
                                           .withTag("tag1", "tag1Value")
                                           .withExcludedTarget(JMX);
        MetricDescriptor sameMetric = metric1.copy();

        compressor.addLong(metric1, 42L);
        compressor.addLong(sameMetric, 43L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumer = new MetricConsumer() {

            @Override
            public void consumeLong(MetricDescriptor descriptor, long value) {
                if (value == 42L) {
                    descriptor.reset()
                              .withPrefix("modifiedPrefix")
                              .withMetric("modifiedMetric")
                              .withDiscriminator("name", "modifiedName")
                              .withUnit(BYTES)
                              .withTag("modifiedTag0", "modifiedTag0Value")
                              .withTag("modifiedTag1", "modifiedTag1Value")
                              .withExcludedTarget(DIAGNOSTICS);
                }
            }

            @Override
            public void consumeDouble(MetricDescriptor descriptor, double value) {

            }
        };

        MetricConsumer metricConsumerSpy = spy(metricConsumer);
        MetricsCompressor.extractMetrics(blob, metricConsumerSpy, supplierSpy);

        verify(metricConsumerSpy).consumeLong(sameMetric, 43L);
    }

    @Test
    public void testLongTagValue() {
        DefaultMetricDescriptorSupplier supplier = new DefaultMetricDescriptorSupplier();
        MetricsCompressor compressor = new MetricsCompressor();

        String longPrefix = Stream.generate(() -> "a")
                                  .limit(MetricsDictionary.MAX_WORD_LENGTH - 1)
                                  .collect(Collectors.joining());
        MetricDescriptor metric1 = supplier.get()
                                           .withMetric("metricName")
                                           .withTag("tag0", longPrefix + "0")
                                           .withTag("tag1", longPrefix + "1");

        compressor.addLong(metric1, 42);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumer = new MetricConsumer() {
            @Override
            public void consumeLong(MetricDescriptor descriptor, long value) {
                assertEquals(42, value);
                assertEquals("metricName", descriptor.metric());
                assertEquals(longPrefix + "0", descriptor.tagValue("tag0"));
                assertEquals(longPrefix + "1", descriptor.tagValue("tag1"));
            }

            @Override
            public void consumeDouble(MetricDescriptor descriptor, double value) {
                fail("Restored a double metric");
            }
        };

        MetricConsumer metricConsumerSpy = spy(metricConsumer);
        MetricsCompressor.extractMetrics(blob, metricConsumerSpy, supplierSpy);
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__tagName() {
        when_tooLongWord_then_metricIgnored(supplier.get().withTag(LONG_NAME, "a"));
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__tagValue() {
        when_tooLongWord_then_metricIgnored(supplier.get().withTag("n", LONG_NAME));
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__metricName() {
        when_tooLongWord_then_metricIgnored(supplier.get().withMetric(LONG_NAME));
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__prefix() {
        when_tooLongWord_then_metricIgnored(supplier.get().withPrefix(LONG_NAME));
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__discriminatorTag() {
        when_tooLongWord_then_metricIgnored(supplier.get().withDiscriminator(LONG_NAME, "a"));
    }

    @Test
    public void when_tooLongWord_then_metricIgnored__discriminatorValue() {
        when_tooLongWord_then_metricIgnored(supplier.get().withDiscriminator("n", LONG_NAME));
    }

    @Test
    public void testNewUnitIsConvertedToTag() {
        boolean isNewUnitIntroduced = false;
        ProbeUnit aNewUnit = null;
        for (ProbeUnit unit : ProbeUnit.values()) {
            if (unit.isNewUnit()) {
                isNewUnitIntroduced = true;
                aNewUnit = unit;
            }
        }
        assumeTrue(isNewUnitIntroduced);

        MetricDescriptor originalMetric = supplier.get()
                .withPrefix("prefix")
                .withMetric("metricName")
                .withDiscriminator("ds", "dsName1")
                .withUnit(aNewUnit)
                .withTag("tag0", "tag0Value");

        compressor.addLong(originalMetric, 42L);
        byte[] blob = compressor.getBlobAndReset();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);

        MetricDescriptor expectedMetric = supplier.get()
                .withPrefix("prefix")
                .withMetric("metricName")
                .withDiscriminator("ds", "dsName1")
                .withUnit(null)
                .withTag("tag0", "tag0Value")
                .withTag("metric-unit", aNewUnit.name());

        verify(metricConsumerMock).consumeLong(expectedMetric, 42L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, only()).get();
    }

    private void when_tooLongWord_then_metricIgnored(MetricDescriptor badDescriptor) {
        MetricsCompressor compressor = new MetricsCompressor();
        try {
            compressor.addLong(badDescriptor, 42);
            fail("should have failed");
        } catch (LongWordException ignored) {
        }
        assertEquals(0, compressor.count());
        // add a good descriptor after a bad one to check that the tmp streams are reset properly
        MetricDescriptor goodDescriptor = supplier.get();
        compressor.addLong(goodDescriptor, 43);
        assertEquals(1, compressor.count());
        byte[] blob = compressor.getBlobAndReset();

        // try to decompress the metrics to see that a valid data were produced
        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        MetricsCompressor.extractMetrics(blob, metricConsumerMock, supplierSpy);
        verify(metricConsumerMock).consumeLong(goodDescriptor, 43L);
        verifyNoMoreInteractions(metricConsumerMock);
        verify(supplierSpy, times(1)).get();
    }
}
