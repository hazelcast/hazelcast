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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.MetricConsumer;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.JmxLeakHelper;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static com.hazelcast.internal.metrics.impl.MetricsCompressor.extractMetrics;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MetricsServiceTest extends HazelcastTestSupport {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock
    private Node nodeMock;
    @Mock
    private HazelcastInstance hzMock;
    @Mock
    private NodeEngineImpl nodeEngineMock;
    @Mock
    private LoggingService loggingServiceMock;
    @Mock
    private ILogger loggerMock;
    @Mock
    private MetricsCollector metricsCollectorMock;

    private MetricsRegistry metricsRegistry;
    private TestProbeSource testProbeSource;
    private final Config config = new Config();
    private ExecutionServiceImpl executionService;

    private MetricsService metricsService;

    @Before
    public void setUp() {
        initMocks(this);

        metricsRegistry = new MetricsRegistryImpl(loggerMock, ProbeLevel.INFO);

        when(nodeMock.getLogger(any(Class.class))).thenReturn(loggerMock);
        when(nodeMock.getLogger(any(String.class))).thenReturn(loggerMock);
        when(nodeEngineMock.getNode()).thenReturn(nodeMock);
        when(nodeEngineMock.getConfig()).thenReturn(config);
        when(nodeEngineMock.getLoggingService()).thenReturn(loggingServiceMock);
        when(nodeEngineMock.getLogger(any(Class.class))).thenReturn(loggerMock);
        when(nodeEngineMock.getMetricsRegistry()).thenReturn(metricsRegistry);
        when(nodeEngineMock.getHazelcastInstance()).thenReturn(hzMock);
        when(hzMock.getName()).thenReturn("mockInstance");

        executionService = new ExecutionServiceImpl(nodeEngineMock);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionService);

        when(loggingServiceMock.getLogger(any(Class.class))).thenReturn(loggerMock);

        testProbeSource = new TestProbeSource();

        metricsRegistry.registerStaticMetrics(testProbeSource, "test");
    }

    @After
    public void tearDown() {
        // Destroy JMX beans created during testing.
        if (metricsService != null) {
            metricsService.shutdown(true);

            metricsService = null;
        }

        JmxLeakHelper.checkJmxBeans();

        // Stop executor service.
        if (executionService != null) {
            executionService.shutdown();
        }
    }

    @Test
    public void testUpdatesRenderedInOrder() {
        MetricsService metricsService = prepareMetricsService();

        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics(metricsCollectorMock);

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics(metricsCollectorMock);

        InOrder inOrderLong = inOrder(metricsCollectorMock);
        InOrder inOrderDouble = inOrder(metricsCollectorMock);

        MetricDescriptor descRoot = metricsRegistry.newMetricDescriptor()
                                                   .withPrefix("test")
                                                   .withUnit(COUNT);
        MetricDescriptor descLongValue = descRoot.copy().withMetric("longValue");
        inOrderLong.verify(metricsCollectorMock).collectLong(descLongValue, 1);
        inOrderLong.verify(metricsCollectorMock).collectLong(descLongValue, 2);
        inOrderLong.verify(metricsCollectorMock, never()).collectLong(eq(descLongValue), anyLong());

        MetricDescriptor descDoubleValue = descRoot.copy().withMetric("doubleValue");
        inOrderDouble.verify(metricsCollectorMock).collectDouble(descDoubleValue, 1.5D);
        inOrderDouble.verify(metricsCollectorMock).collectDouble(descDoubleValue, 5.5D);
        inOrderDouble.verify(metricsCollectorMock, never()).collectDouble(eq(descDoubleValue), anyDouble());
    }

    @Test
    public void testReadMetricsReadsLastTwoCollections() throws Exception {
        // configure metrics to keep the result of the last 2 collection cycles
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(1)
              .getManagementCenterConfig()
              .setRetentionSeconds(2);

        MetricsService metricsService = prepareMetricsService();

        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics();

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        InOrder inOrderLong = inOrder(metricConsumerMock);
        InOrder inOrderDouble = inOrder(metricConsumerMock);

        readMetrics(metricsService, 0, metricConsumerMock);

        MetricDescriptor longDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("longValue")
                .withUnit(COUNT);
        inOrderLong.verify(metricConsumerMock).consumeLong(longDescriptor, 1);
        inOrderLong.verify(metricConsumerMock).consumeLong(longDescriptor, 2);
        inOrderLong.verify(metricConsumerMock, never()).consumeLong(eq(longDescriptor), anyLong());

        MetricDescriptor doubleDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("doubleValue")
                .withUnit(COUNT);
        inOrderDouble.verify(metricConsumerMock).consumeDouble(doubleDescriptor, 1.5D);
        inOrderDouble.verify(metricConsumerMock).consumeDouble(doubleDescriptor, 5.5D);
        inOrderDouble.verify(metricConsumerMock, never()).consumeDouble(eq(doubleDescriptor), anyDouble());
    }

    @Test
    public void testReadMetricsReadsOnlyLastCollection() throws Exception {
        // configure metrics to keep the result only of the last collection cycle
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(5)
              .getManagementCenterConfig()
              .setRetentionSeconds(1);

        MetricsService metricsService = prepareMetricsService();

        // this collection will be dropped
        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics();

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);

        readMetrics(metricsService, 0, metricConsumerMock);

        InOrder inOrderLong = inOrder(metricConsumerMock);
        InOrder inOrderDouble = inOrder(metricConsumerMock);

        MetricDescriptor doubleDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("doubleValue")
                .withUnit(COUNT);
        inOrderDouble.verify(metricConsumerMock).consumeDouble(doubleDescriptor, 5.5D);
        inOrderDouble.verify(metricConsumerMock, never()).consumeDouble(eq(doubleDescriptor), anyDouble());

        MetricDescriptor longDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("longValue")
                .withUnit(COUNT);
        inOrderLong.verify(metricConsumerMock).consumeLong(longDescriptor, 2);
        inOrderLong.verify(metricConsumerMock, never()).consumeLong(eq(longDescriptor), anyLong());
    }

    @Test
    public void testNoCollectionIfMetricsDisabled() {
        config.getMetricsConfig().setEnabled(false);
        ExecutionService executionServiceMock = mock(ExecutionService.class);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionServiceMock);

        prepareMetricsService();

        verifyNoMoreInteractions(executionServiceMock);
    }

    @Test
    public void testNoCollectionIfMetricsEnabledAndMcJmxDisabled() {
        config.getMetricsConfig()
              .setEnabled(true);
        config.getMetricsConfig().getManagementCenterConfig()
              .setEnabled(false);
        config.getMetricsConfig().getJmxConfig()
              .setEnabled(false);

        ExecutionService executionServiceMock = mock(ExecutionService.class);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionServiceMock);

        prepareMetricsService();

        verifyNoMoreInteractions(executionServiceMock);
    }

    @Test
    public void testMetricsCollectedIfMetricsEnabledAndMcJmxDisabledButCustomPublisherRegistered() {
        config.getMetricsConfig()
              .setEnabled(true);
        config.getMetricsConfig().getManagementCenterConfig()
              .setEnabled(false);
        config.getMetricsConfig().getJmxConfig()
              .setEnabled(false);

        MetricsPublisher publisherMock = mock(MetricsPublisher.class);
        MetricsService metricsService = prepareMetricsService();
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        assertTrueEventually(() -> {
            verify(publisherMock, atLeastOnce()).publishDouble(any(), anyDouble());
            verify(publisherMock, atLeastOnce()).publishLong(any(), anyLong());
        });
    }

    @Test
    public void testReadMetricsThrowsOnFutureSequence() throws Exception {
        MetricsService metricsService = prepareMetricsService();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);

        long futureSequence = 42;
        long headSequence = 0;

        expectedException.expect(ExecutionException.class);
        expectedException.expectCause(Is.is(CoreMatchers.instanceOf(IllegalArgumentException.class)));
        expectedException.expectMessage(Long.toString(futureSequence));
        expectedException.expectMessage(Long.toString(headSequence));
        readMetrics(metricsService, futureSequence, metricConsumerMock);
    }

    @Test
    public void testCustomPublisherIsRegistered() {
        MetricsPublisher publisherMock = mock(MetricsPublisher.class);
        MetricsService metricsService = prepareMetricsService();
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        metricsService.collectMetrics();

        verify(publisherMock, atLeastOnce()).publishDouble(any(), anyDouble());
        verify(publisherMock, atLeastOnce()).publishLong(any(), anyLong());
    }

    @Test
    public void testCustomPublisherIsNotRegisteredIfMetricsDisabled() {
        config.getMetricsConfig().setEnabled(false);

        MetricsPublisher publisherMock = mock(MetricsPublisher.class);
        MetricsService metricsService = prepareMetricsService();
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        metricsService.collectMetrics();

        verify(publisherMock, never()).publishDouble(any(), anyDouble());
        verify(publisherMock, never()).publishLong(any(), anyLong());
    }

    @Test
    public void testExclusion() throws Exception {
        // configure metrics to keep the result only of the last collection cycle
        config.getMetricsConfig()
              .setCollectionFrequencySeconds(5)
              .getManagementCenterConfig()
              .setRetentionSeconds(1);

        ExclusionProbeSource metricsSource = new ExclusionProbeSource();
        metricsRegistry.registerStaticMetrics(metricsSource, "testExclusion");

        MetricsService metricsService = prepareMetricsService();

        metricsSource.update(1, 2, 1.5D, 2.5D);
        metricsService.collectMetrics();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        readMetrics(metricsService, 0, metricConsumerMock);

        MetricDescriptor longRoot = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("testExclusion")
                .withUnit(COUNT);
        verify(metricConsumerMock).consumeLong(longRoot.copy().withMetric("notExcludedLong"), 1);
        verify(metricConsumerMock, never()).consumeLong(eq(longRoot.copy().withMetric("excludedLong")), anyLong());

        MetricDescriptor doubleRoot = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("testExclusion")
                .withUnit(COUNT);
        verify(metricConsumerMock).consumeDouble(doubleRoot.copy().withMetric("notExcludedDouble"), 1.5D);
        verify(metricConsumerMock, never()).consumeDouble(eq(doubleRoot.copy().withMetric("excludedDouble")), anyDouble());
    }

    private void readMetrics(MetricsService metricsService, long sequence, MetricConsumer metricConsumer)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future = metricsService.readMetrics(sequence);
        RingbufferSlice<Map.Entry<Long, byte[]>> ringbufferSlice = future.get();

        MetricsResultSet metricsResultSet = new MetricsResultSet(ringbufferSlice.nextSequence(), ringbufferSlice.elements());
        metricsResultSet.collections().forEach(entry -> extractMetrics(entry.getValue(), metricConsumer));
    }

    private MetricsService prepareMetricsService() {
        if (metricsService != null) {
            throw new IllegalStateException("MetricsService is already prepared.");
        }

        metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        return metricsService;
    }

    private static class TestProbeSource {
        @Probe(name = "longValue")
        private long longValue;

        @Probe(name = "doubleValue")
        private double doubleValue;

        private void update(long longValue, double doubleValue) {
            this.longValue = longValue;
            this.doubleValue = doubleValue;
        }

    }

    private static class ExclusionProbeSource {
        @Probe(name = "notExcludedLong")
        private long notExcludedLong;

        @Probe(name = "excludedLong", excludedTargets = MANAGEMENT_CENTER)
        private long excludedLong;

        @Probe(name = "notExcludedDouble")
        private double notExcludedDouble;

        @Probe(name = "excludedDouble", excludedTargets = MANAGEMENT_CENTER)
        private double excludedDouble;

        private void update(long notExcludedLong, long excludedLong, double notExcludedDouble, double excludedDouble) {
            this.notExcludedLong = notExcludedLong;
            this.excludedLong = excludedLong;
            this.notExcludedDouble = notExcludedDouble;
            this.excludedDouble = excludedDouble;
        }
    }
}
