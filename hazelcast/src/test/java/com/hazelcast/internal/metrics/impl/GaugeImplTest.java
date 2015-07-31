package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.DoubleProbe;
import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.LongProbe;
import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.lang.Math.round;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class GaugeImplTest {

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

    @Test
    public void getName() {
        Gauge gauge = metricsRegistry.getGauge("foo");

        String actual = gauge.getName();

        assertEquals("foo", actual);
    }

    //  ============ getLong ===========================

    @Test
    public void readLong_whenNoInput() {
        Gauge gauge = metricsRegistry.getGauge("foo");

        long actual = gauge.readLong();

        assertEquals(0, actual);
    }

    @Test
    public void readLong_whenDoubleGauge() {
        metricsRegistry.register(this, "foo", new DoubleProbe<GaugeImplTest>() {
            @Override
            public double get(GaugeImplTest source) throws Exception {
                return 10;
            }
        });

        Gauge gauge = metricsRegistry.getGauge("foo");

        long actual = gauge.readLong();

        assertEquals(10, actual);
    }

    @Test
    public void readLong_whenLongGauge() {
        metricsRegistry.register(this, "foo", new LongProbe() {
            @Override
            public long get(Object o) throws Exception {
                return 10;
            }
        });

        Gauge gauge = metricsRegistry.getGauge("foo");
        assertEquals(10, gauge.readLong());
    }

    @Test
    public void readLong_whenExceptionalInput() {
        metricsRegistry.register(this, "foo", new LongProbe() {
            @Override
            public long get(Object o) {
                throw new RuntimeException();
            }
        });

        Gauge gauge = metricsRegistry.getGauge("foo");

        long actual = gauge.readLong();

        assertEquals(0, actual);
    }

    @Test
    public void readLong_whenLongGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.longField");
        assertEquals(10, gauge.readLong());
    }

    @Test
    public void readLong_whenDoubleGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.doubleField");
        assertEquals(round(someObject.doubleField), gauge.readLong());
    }

    // ============ readDouble ===========================

    @Test
    public void readDouble_whenNoMetricInput() {
        Gauge gauge = metricsRegistry.getGauge("foo");

        double actual = gauge.readDouble();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void readDouble_whenExceptionalInput() {
        metricsRegistry.register(this, "foo", new DoubleProbe() {
            @Override
            public double get(Object o) {
                throw new RuntimeException();
            }
        });

        Gauge gauge = metricsRegistry.getGauge("foo");

        double actual = gauge.readDouble();

        assertEquals(0, actual, 0.1);
    }

    @Test
    public void readDouble_whenDoubleGauge() {
        metricsRegistry.register(this, "foo", new DoubleProbe() {
            @Override
            public double get(Object o) {
                return 10;
            }
        });
        Gauge gauge = metricsRegistry.getGauge("foo");

        double actual = gauge.readDouble();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void readDouble_whenLongGauge() {
        metricsRegistry.register(this, "foo", new LongProbe() {
            @Override
            public long get(Object o) throws Exception {
                return 10;
            }
        });
        Gauge gauge = metricsRegistry.getGauge("foo");

        double actual = gauge.readDouble();

        assertEquals(10, actual, 0.1);
    }

    @Test
    public void readDouble_whenLongGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.longField");
        assertEquals(someObject.longField, gauge.readDouble(), 0.1);
    }

    @Test
    public void readDouble_whenDoubleGaugeField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");

        Gauge gauge = metricsRegistry.getGauge("foo.doubleField");
        assertEquals(someObject.doubleField, gauge.readDouble(), 0.1);
    }

    // ====================== render ===================================

    @Test
    public void render_whenDoubleGauge() {
        metricsRegistry.register(this, "foo", new DoubleProbe() {
            @Override
            public double get(Object o) {
                return 10;
            }
        });
        Gauge gauge = metricsRegistry.getGauge("foo");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);

        assertEquals("foo=10.0", sb.toString());
    }

    @Test
    public void render_whenLongGauge() {
        metricsRegistry.register(this, "foo", new LongProbe() {
            @Override
            public long get(Object o) {
                return 10;
            }
        });
        Gauge gauge = metricsRegistry.getGauge("foo");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);

        assertEquals("foo=10", sb.toString());
    }

    @Test
    public void render_whenNoInput() {
        Gauge gauge = metricsRegistry.getGauge("foo");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);

        assertEquals("foo=NA", sb.toString());
    }

    @Test
    public void render_whenNoSource() {
        Gauge gauge = metricsRegistry.getGauge("foo");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);

        assertEquals("foo=NA", sb.toString());
    }

    @Test
    public void render_whenDoubleField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");
        Gauge gauge = metricsRegistry.getGauge("foo.doubleField");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);
        assertEquals("foo.doubleField=10.8", sb.toString());
    }

    @Test
    public void render_whenLongField() {
        SomeObject someObject = new SomeObject();
        metricsRegistry.scanAndRegister(someObject, "foo");
        Gauge gauge = metricsRegistry.getGauge("foo.longField");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);
        assertEquals("foo.longField=10", sb.toString());
    }

    @Test
    public void render_whenException() {
        metricsRegistry.register(this, "foo", new LongProbe() {
            @Override
            public long get(Object o) {
                throw new RuntimeException();
            }
        });
        Gauge gauge = metricsRegistry.getGauge("foo");
        StringBuilder sb = new StringBuilder();

        gauge.render(sb);

        assertEquals("foo=NA", sb.toString());
    }
}
