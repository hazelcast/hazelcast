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
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricDescriptor;
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
import static java.lang.Math.round;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LongGaugeImplTest extends HazelcastTestSupport {

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

    @Test
    public void getName() {
        LongGauge gauge = metricsRegistry.newLongGauge("foo[id].bar");

        String actual = gauge.getName();

        assertEquals("foo[id].bar", actual);
    }

    //  ============ getLong ===========================

    @Test
    public void whenNoProbeSet() {
        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(0, actual);
    }

    @Test
    public void whenDoubleProbe() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (DoubleProbeFunction<LongGaugeImplTest>) source -> 10);

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(10, actual);
    }

    @Test
    public void whenLongProbe() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY, (LongProbeFunction) o -> 10);

        LongGauge gauge = metricsRegistry.newLongGauge("foo");
        assertEquals(10, gauge.read());
    }

    @Test
    public void whenProbeThrowsException() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (LongProbeFunction) o -> {
                    throw new RuntimeException();
                });

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        long actual = gauge.read();

        assertEquals(0, actual);
    }

    @Test
    public void whenLongProbeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.longField");
        assertEquals(10, gauge.read());
    }

    @Test
    public void whenDoubleProbeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        LongGauge gauge = metricsRegistry.newLongGauge("foo.doubleField");
        assertEquals(round(someObject.doubleField), gauge.read());
    }

    @Test
    public void whenReregister() {
        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (LongProbeFunction) o -> 10);

        LongGauge gauge = metricsRegistry.newLongGauge("foo");

        gauge.read();

        metricsRegistry.registerStaticProbe(this, "foo", MANDATORY,
                (LongProbeFunction) o -> 11);

        assertEquals(11, gauge.read());
    }

    @Test
    public void whenCreatedForDynamicLongMetricWithExtractedValue() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42, longGauge.read());

        someObject.longField = 43;
        assertEquals(43, longGauge.read());
    }

    @Test
    public void whenCreatedForDynamicDoubleMetricWithExtractedValue() {
        SomeObject someObject = new SomeObject();
        someObject.doubleField = 41.65D;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42, longGauge.read());

        someObject.doubleField = 42.65D;
        assertEquals(43, longGauge.read());
    }

    @Test
    public void whenCreatedForDynamicLongMetricWithProvidedValue() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider((descriptor, context) -> context
                .collect(descriptor.withPrefix("foo"), "longField", INFO, BYTES, 42));
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42, longGauge.read());
    }

    @Test
    public void whenCreatedForDynamicDoubleMetricWithProvidedValue() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider((tagger, context) -> context
                .collect(tagger.withPrefix("foo"), "doubleField", INFO, BYTES, 41.65D));
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.doubleField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(42, longGauge.read());
    }

    @Test
    public void whenCacheDynamicMetricSourceGcdReadsDefault() {
        NullableDynamicMetricsProvider metricsProvider = new NullableDynamicMetricsProvider();
        WeakReference<SomeObject> someObjectWeakRef = new WeakReference<>(metricsProvider.someObject);
        metricsProvider.someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(metricsProvider);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42, longGauge.read());

        metricsProvider.someObject = null;
        System.gc();
        // wait for someObject to get GCd - should have already happened
        assertTrueEventually(() -> assertNull(someObjectWeakRef.get()));

        assertEquals(LongGaugeImpl.DEFAULT_VALUE, longGauge.read());
    }

    @Test
    public void whenCacheDynamicMetricSourceReplacedWithConcreteValue() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42, longGauge.read());

        metricsRegistry.deregisterDynamicMetricsProvider(someObject);

        metricsRegistry.registerDynamicMetricsProvider((descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "longField", INFO, COUNT, 142));
        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(142, longGauge.read());
    }

    @Test
    public void whenCacheDynamicMetricValueReplacedWithCachedMetricSource() {
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // provide concrete value
        DynamicMetricsProvider concreteProvider = (descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "longField", INFO, COUNT, 142);
        metricsRegistry.registerDynamicMetricsProvider(concreteProvider);
        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(142, longGauge.read());

        metricsRegistry.deregisterDynamicMetricsProvider(concreteProvider);

        // provide metric source to be cached
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(someObject);

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42, longGauge.read());
    }

    @Test
    public void whenNotVisitedWithCachedMetricSourceReadsDefault() {
        SomeObject someObject = new SomeObject();
        someObject.longField = 42;
        metricsRegistry.registerDynamicMetricsProvider(someObject);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42, longGauge.read());

        // clears the cached metric source
        metricsRegistry.deregisterDynamicMetricsProvider(someObject);
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(LongGaugeImpl.DEFAULT_VALUE, longGauge.read());
    }

    @Test
    public void whenNotVisitedWithCachedValueReadsDefault() {
        DynamicMetricsProvider concreteProvider = (descriptor, context) ->
                context.collect(descriptor.withPrefix("foo"), "longField", INFO, COUNT, 42);
        metricsRegistry.registerDynamicMetricsProvider(concreteProvider);
        LongGaugeImpl longGauge = metricsRegistry.newLongGauge("foo.longField");

        // needed to collect dynamic metrics and update the gauge created from them
        metricsRegistry.collect(mock(MetricsCollector.class));
        assertEquals(42, longGauge.read());

        // clears the cached metric source
        metricsRegistry.deregisterDynamicMetricsProvider(concreteProvider);
        metricsRegistry.collect(mock(MetricsCollector.class));

        assertEquals(LongGaugeImpl.DEFAULT_VALUE, longGauge.read());
    }

    @Test
    public void whenProbeRegisteredAfterGauge() {
        LongGauge gauge = metricsRegistry.newLongGauge("foo.longField");

        SomeObject someObject = new SomeObject();
        metricsRegistry.registerStaticMetrics(someObject, "foo");

        assertEquals(someObject.longField, gauge.read());
    }
}
