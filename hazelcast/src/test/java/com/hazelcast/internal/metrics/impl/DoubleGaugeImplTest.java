package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.DoubleProbeFunction;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class DoubleGaugeImplTest {
    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    class SomeObject {
        @Probe
        long longField = 10;
        @Probe
        double doubleField = 10.8;
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
        metricsRegistry.register(this, "foo", new DoubleProbeFunction() {
            @Override
            public double get(Object o) {
                throw new RuntimeException();
            }
        });

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void whenDoubleProbe() {
        metricsRegistry.register(this, "foo", new DoubleProbeFunction() {
            @Override
            public double get(Object o) {
                return 10;
            }
        });
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void whenLongProbe() {
        metricsRegistry.register(this, "foo", new LongProbeFunction() {
            @Override
            public long get(Object o) throws Exception {
                return 10;
            }
        });
        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo");

        double actual = gauge.read();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void whenLongGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.longField");
        assertEquals(someObject.longField, gauge.read(), 0.1);
    }

    @Test
    public void whenDoubleGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("foo.doubleField");
        assertEquals(someObject.doubleField, gauge.read(), 0.1);
    }
}
