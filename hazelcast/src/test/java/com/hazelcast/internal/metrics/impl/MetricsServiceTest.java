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

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.metrics.MetricsPublisher;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.managementcenter.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.internal.metrics.managementcenter.MetricsResultSet;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(HazelcastParallelClassRunner.class)
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
    private ProbeRenderer probeRendererMock;

    private MetricsRegistry metricsRegistry;
    private TestProbeSource testProbeSource;
    private final Config config = new Config();

    @Before
    public void setUp() {
        initMocks(this);

        config.getMetricsConfig().setCollectionIntervalSeconds(10);
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

        ExecutionServiceImpl executionService = new ExecutionServiceImpl(nodeEngineMock);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionService);

        when(loggingServiceMock.getLogger(any(Class.class))).thenReturn(loggerMock);

        testProbeSource = new TestProbeSource();

        metricsRegistry.scanAndRegister(testProbeSource, "test");
    }

    @Test
    public void testUpdatesRenderedInOrder() {
        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics(probeRendererMock);

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics(probeRendererMock);

        InOrder inOrder = inOrder(probeRendererMock);
        inOrder.verify(probeRendererMock).renderDouble("test.doubleValue", 1.5D);
        inOrder.verify(probeRendererMock).renderLong("test.longValue", 1);
        inOrder.verify(probeRendererMock).renderDouble("test.doubleValue", 5.5D);
        inOrder.verify(probeRendererMock).renderLong("test.longValue", 2);
    }

    @Test
    public void testReadMetricsReadsLastTwoCollections() throws Exception {
        // configure metrics to keep the result of the last 2 collection cycles
        config.getMetricsConfig()
              .setRetentionSeconds(2)
              .setCollectionIntervalSeconds(1);

        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics();

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        InOrder inOrder = inOrder(metricConsumerMock);

        readMetrics(metricsService, 0, metricConsumerMock);

        inOrder.verify(metricConsumerMock).consumeDouble(1.5D);
        inOrder.verify(metricConsumerMock).consumeLong(1);
        inOrder.verify(metricConsumerMock).consumeDouble(5.5D);
        inOrder.verify(metricConsumerMock).consumeLong(2);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testReadMetricsReadsOnlyLastCollection() throws Exception {
        // configure metrics to keep the result only of the last collection cycle
        config.getMetricsConfig()
              .setRetentionSeconds(1)
              .setCollectionIntervalSeconds(5);

        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        // this collection will be dropped
        testProbeSource.update(1, 1.5D);
        metricsService.collectMetrics();

        testProbeSource.update(2, 5.5D);
        metricsService.collectMetrics();

        MetricConsumer metricConsumerMock = mock(MetricConsumer.class);
        InOrder inOrder = inOrder(metricConsumerMock);

        readMetrics(metricsService, 0, metricConsumerMock);

        inOrder.verify(metricConsumerMock).consumeDouble(5.5D);
        inOrder.verify(metricConsumerMock).consumeLong(2);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void testNoCollectionIfMetricsDisabled() {
        config.getMetricsConfig().setEnabled(false);
        ExecutionService executionServiceMock = mock(ExecutionService.class);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionServiceMock);

        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        verifyNoMoreInteractions(executionServiceMock);
    }

    @Test
    public void testNoCollectionIfMetricsEnabledAndMcJmxDisabled() {
        config.getMetricsConfig()
              .setEnabled(true)
              .setMcEnabled(false)
              .setJmxEnabled(false);
        ExecutionService executionServiceMock = mock(ExecutionService.class);
        when(nodeEngineMock.getExecutionService()).thenReturn(executionServiceMock);

        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

        verifyNoMoreInteractions(executionServiceMock);
    }

    @Test
    public void testMetricsCollectedIfMetricsEnabledAndMcJmxDisabledButCustomPublisherRegistered() {
        config.getMetricsConfig()
              .setEnabled(true)
              .setMcEnabled(false)
              .setJmxEnabled(false);

        MetricsPublisher publisherMock = mock(MetricsPublisher.class);
        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        assertTrueEventually(() -> {
            verify(publisherMock, atLeastOnce()).publishDouble(anyString(), anyDouble());
            verify(publisherMock, atLeastOnce()).publishLong(anyString(), anyLong());
        });
    }

    @Test
    public void testReadMetricsThrowsOnFutureSequence() throws Exception {
        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());

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
        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        metricsService.collectMetrics();

        verify(publisherMock, atLeastOnce()).publishDouble(anyString(), anyDouble());
        verify(publisherMock, atLeastOnce()).publishLong(anyString(), anyLong());
    }

    @Test
    public void testCustomPublisherIsNotRegisteredIfMetricsDisabled() {
        config.getMetricsConfig().setEnabled(false);

        MetricsPublisher publisherMock = mock(MetricsPublisher.class);
        MetricsService metricsService = new MetricsService(nodeEngineMock, () -> metricsRegistry);
        metricsService.init(nodeEngineMock, new Properties());
        metricsService.registerPublisher(nodeEngine -> publisherMock);

        metricsService.collectMetrics();

        verify(publisherMock, never()).publishDouble(anyString(), anyDouble());
        verify(publisherMock, never()).publishLong(anyString(), anyLong());
    }

    private void readMetrics(MetricsService metricsService, long sequence, MetricConsumer metricConsumer)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        CompletableFuture<RingbufferSlice<Map.Entry<Long, byte[]>>> future = metricsService.readMetrics(sequence);
        RingbufferSlice<Map.Entry<Long, byte[]>> ringbufferSlice = future.get();

        MetricsResultSet metricsResultSet = new MetricsResultSet(ringbufferSlice.nextSequence(), ringbufferSlice.elements());

        metricsResultSet.collections().forEach(coll -> coll.forEach(metric -> {
            if (metric.key().contains("test.longValue")) {
                metricConsumer.consumeLong(metric.value());
            } else if (metric.key().contains("test.doubleValue")) {
                metricConsumer.consumeDouble(metric.value() / 10_000D);
            }
        }));
    }

    private static class TestProbeSource {
        @Probe
        private long longValue;

        @Probe
        private double doubleValue;

        private void update(long longValue, double doubleValue) {
            this.longValue = longValue;
            this.doubleValue = doubleValue;
        }

    }

    private interface MetricConsumer {
        void consumeLong(long value);

        void consumeDouble(double value);
    }
}
