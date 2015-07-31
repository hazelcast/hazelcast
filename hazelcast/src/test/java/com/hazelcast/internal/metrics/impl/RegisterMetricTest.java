package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.Metric;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class RegisterMetricTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test(expected = NullPointerException.class)
    public void whenNamePrefixNull() {
        metricsRegistry.scanAndRegister(new SomeField(), null);
    }

    @Test(expected = NullPointerException.class)
    public void whenObjectNull() {
        metricsRegistry.scanAndRegister(null, "bar");
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenUnrecognizedField() {
        metricsRegistry.scanAndRegister(new SomeUnrecognizedField(), "bar");
    }

    @Test
    public void whenNoGauges_thenIgnore() {
        metricsRegistry.scanAndRegister(new LinkedList(), "bar");

        for (String name : metricsRegistry.getNames()) {
            assertFalse(name.startsWith("bar"));
        }
    }

    public class SomeField {
        @Probe
        long field;
    }

    public class SomeUnrecognizedField {
        @Probe
        OutputStream field;
    }

    @Test
    public void deregister_whenNotRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));
 }

    public class MultiFieldAndMethod {
        @Probe
        long field1;
        @Probe
        long field2;

        @Probe
        int method1() {
            return 1;
        }

        @Probe
        int method2() {
            return 2;
        }
    }

    @Test
    public void deregister_whenRegistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.scanAndRegister(multiFieldAndMethod, "foo");

        Gauge field1 = metricsRegistry.getGauge("foo.field1");
        Gauge field2 = metricsRegistry.getGauge("foo.field2");
        Gauge method1 = metricsRegistry.getGauge("foo.method1");
        Gauge method2 = metricsRegistry.getGauge("foo.method2");

        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));

        // make sure that the metric input has been disconnected
        assertEquals(0, field1.readLong());
        assertEquals(0, field2.readLong());
        assertEquals(0, method1.readLong());
        assertEquals(0, method2.readLong());
    }

    @Test
    public void deregister_whenAlreadyDeregistered() {
        MultiFieldAndMethod multiFieldAndMethod = new MultiFieldAndMethod();
        multiFieldAndMethod.field1 = 1;
        multiFieldAndMethod.field2 = 2;
        metricsRegistry.scanAndRegister(multiFieldAndMethod, "foo");
        metricsRegistry.deregister(multiFieldAndMethod);
        metricsRegistry.deregister(multiFieldAndMethod);

        // make sure that the the metrics have been removed
        Set<String> names = metricsRegistry.getNames();
        assertFalse(names.contains("foo.field1"));
        assertFalse(names.contains("foo.field2"));
        assertFalse(names.contains("foo.method1"));
        assertFalse(names.contains("foo.method2"));
    }
}
