package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MetricsRegistryImplTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void modCount() {
        long modCount = metricsRegistry.modCount();
        metricsRegistry.register(this, "foo", new LongProbeFunction() {
            @Override
            public long get(Object obj) throws Exception {
                return 1;
            }
        });
        assertEquals(modCount + 1, metricsRegistry.modCount());

        metricsRegistry.deregister(this);
        assertEquals(modCount + 2, metricsRegistry.modCount());
    }

    // ================ newLongGauge ======================

    @Test(expected = NullPointerException.class)
    public void newGauge_whenNullName() {
        metricsRegistry.newLongGauge(null);
    }

    @Test
    public void newGauge_whenNotExistingMetric() {
        LongGaugeImpl gauge = metricsRegistry.newLongGauge("foo");

        assertNotNull(gauge);
        assertEquals("foo", gauge.getName());
        assertEquals(0, gauge.read());
    }

    @Test
    public void newGauge_whenExistingMetric() {
        LongGaugeImpl first = metricsRegistry.newLongGauge("foo");
        LongGaugeImpl second = metricsRegistry.newLongGauge("foo");

        assertNotSame(first, second);
    }

    // ================ getNames ======================

    @Test
    public void getNames() {
        Set<String> expected = new HashSet<String>();
        expected.add("first");
        expected.add("second");
        expected.add("third");

        for (String name : expected) {
            metricsRegistry.register(this, name, new LongProbeFunction() {
                @Override
                public long get(Object obj) throws Exception {
                    return 0;
                }
            });
        }

        Set<String> names = metricsRegistry.getNames();
        for (String name : expected) {
            assertTrue(names.contains(name));
        }
    }

    @Test
    public void shutdown() {
        metricsRegistry.shutdown();
    }
}
