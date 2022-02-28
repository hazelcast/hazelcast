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
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import static com.hazelcast.internal.metrics.impl.DefaultMetricDescriptorSupplier.DEFAULT_DESCRIPTOR_SUPPLIER;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CompositeMetricsCollectorTest {

    @Mock
    private MetricsCollector collectorMock1;

    @Mock
    private MetricsCollector collectorMock2;

    private MetricDescriptor metricsDescriptor;

    private CompositeMetricsCollector compositeCollector;

    @Before
    public void setUp() {
        initMocks(this);
        compositeCollector = new CompositeMetricsCollector(collectorMock1, collectorMock2);

        metricsDescriptor = DEFAULT_DESCRIPTOR_SUPPLIER
                .get()
                .withPrefix("test")
                .withMetric("metric");
    }

    @Test
    public void testCollectLong() {
        compositeCollector.collectLong(metricsDescriptor, 42);

        verify(collectorMock1).collectLong(metricsDescriptor, 42);
        verify(collectorMock2).collectLong(metricsDescriptor, 42);
    }

    @Test
    public void testCollectDouble() {
        compositeCollector.collectDouble(metricsDescriptor, 42.42D);

        verify(collectorMock1).collectDouble(metricsDescriptor, 42.42D);
        verify(collectorMock2).collectDouble(metricsDescriptor, 42.42D);
    }

    @Test
    public void testCollectException() {
        Exception ex = new Exception();
        compositeCollector.collectException(metricsDescriptor, ex);

        verify(collectorMock1).collectException(metricsDescriptor, ex);
        verify(collectorMock2).collectException(metricsDescriptor, ex);
    }

    @Test
    public void testCollectNoValue() {
        compositeCollector.collectNoValue(metricsDescriptor);

        verify(collectorMock1).collectNoValue(metricsDescriptor);
        verify(collectorMock2).collectNoValue(metricsDescriptor);
    }
}
