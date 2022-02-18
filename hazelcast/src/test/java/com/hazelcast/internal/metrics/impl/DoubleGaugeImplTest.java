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

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.ref.WeakReference;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.metrics.ProbeUnit.BYTES;
import static com.hazelcast.internal.metrics.ProbeUnit.COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DoubleGaugeImplTest extends HazelcastTestSupport {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    class SomeObject implements DynamicMetricsProvider {
        @Probe(name = "longField")
        long longField = 10;
        @Probe(name = "doubleField")
        double doubleField = 10.8;

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            context.collect(descriptor.withPrefix("foo"), this);
        }
    }

    class NullableDynamicMetricsProvider implements DynamicMetricsProvider {
        SomeObject someObject = new SomeObject();

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            descriptor.withPrefix("foo");
            if (someObject != null) {
                context.collect(descriptor, someObject);
            } else {
                context.collect(descriptor, "longField", INFO, COUNT, 142);
                context.collect(descriptor, "doubleField", INFO, COUNT, 142.42D);
            }
        }
    }

    // ============ readDouble ===========================

    @Test
    public void whenNoProbeAvailable() {
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void whenProbeThrowsException() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (DoubleProbeFunction) o -> {
                    throw new RuntimeException();
                });

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void whenDoubleProbe() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (DoubleProbeFunction) o -> 10);
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void whenLongProbe() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (LongProbeFunction) o -> 10);
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void whenLongGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.longField");
        assertEquals(someObject.longField, gauge.read(), 0.1);
    }

    @Test
    public void whenDoubleGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.doubleField");
        assertEquals(someObject.doubleField, gauge.read(), 0.1);
    }

    @Test
    public void whenCreatedForDynamicDoubleMetricWithExtractedValue() {
        SomeObject someObject = new SomeObject();
        someObject.doubleField = 41.65D;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(41.65D, doubleGauge.read(), 10E-6);

        someObject.doubleField = 42.65D;
        assertEquals(42.65D, doubleGauge.read(), 10E-6);

    }

    @Test
    public void whenCreatedForDynamicLongMetricWithExtractedValue() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42D, doubleGauge.read(), 10E-6);

        someObject.longField = 43;
        assertEquals(43D, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenCreatedForDynamicLongMetricWithProvidedValue() {
        DoubleGaugeImplTest.SomeObject someObject = new DoubleGaugeImplTest.SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider((descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "longField", INFO, BYTES, 42));
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenCreatedForDynamicDoubleMetricWithProvidedValue() {
        DoubleGaugeImplTest.SomeObject someObject = new DoubleGaugeImplTest.SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider((descriptor, context) -> context
                .collect(descriptor.withPrefix("foo"), "doubleField", INFO, BYTES, 41.65D));
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(41.65, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenCacheDynamicMetricSourceGcdReadsDefault() {
        NullableDynamicMetricsProvider metricsProvider = new NullableDynamicMetricsProvider();
        WeakReference<SomeObject> someObjectWeakRef = new WeakReference<>(metricsProvider.someObject);
        metricsProvider.someObject.doubleField = 42.42D;
        metricsRegistry.registerDynamicMetricsProvider(metricsProvider);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42.42D, doubleGauge.read(), 10E-6);

        metricsProvider.someObject = null;
        System.gc();
        // wait for someObject to get GCd - should have already happened
        assertTrueEventually(() -> assertNull(someObjectWeakRef.get()));

        assertEquals(DoubleGaugeImpl.DEFAULT_VALUE, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenCacheDynamicMetricSourceReplacedWithConcreteValue() {
        SomeObject someObject = new SomeObject();
        someObject.doubleField = 42.42D;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42.42D, doubleGauge.read(), 10E-6);

        metricsRegistry.deregisterDynamicMetricsProvider(someObject);

        metricsRegistry.registerDynamicMetricsProvider((descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "doubleField", INFO, COUNT, 142.42D));
        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(142.42D, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenCacheDynamicMetricValueReplacedWithCachedMetricSource() {
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // provide concrete value
        DynamicMetricsProvider concreteProvider = (descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "doubleField", INFO, COUNT, 142.42D);
        metricsRegistry.registerDynamicMetricsProvider(concreteProvider);
        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(142.42D, doubleGauge.read(), 10E-6);

        metricsRegistry.deregisterDynamicMetricsProvider(concreteProvider);

        // provide metric source to be cached
        SomeObject someObject = new SomeObject();
        someObject.doubleField = 42.42D;
        metricsRegistry.registerDynamicMetricsProvider(someObject);

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42.42D, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenNotVisitedWithCachedMetricSourceReadsDefault() {
        DoubleGaugeImplTest.SomeObject someObject = new DoubleGaugeImplTest.SomeObject();
        someObject.doubleField = 42.42D;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42.42D, doubleGauge.read(), 10E-6);

        // clears the cached metric source
        metricsRegistry.deregisterDynamicMetricsProvider(someObject);
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(DoubleGaugeImpl.DEFAULT_VALUE, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenNotVisitedWithCachedValueReadsDefault() {
        DynamicMetricsProvider concreteProvider = (descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "doubleField", INFO, COUNT, 42.42D);
        metricsRegistry.registerDynamicMetricsProvider(concreteProvider);
        DoubleGauge doubleGauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42.42D, doubleGauge.read(), 10E-6);

        // clears the cached metric source
        metricsRegistry.deregisterDynamicMetricsProvider(concreteProvider);
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(DoubleGaugeImpl.DEFAULT_VALUE, doubleGauge.read(), 10E-6);
    }

    @Test
    public void whenProbeRegisteredAfterGauge() {
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.doubleField");

        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        assertEquals(someObject.doubleField, gauge.read(), 10E-6);
    }

}
