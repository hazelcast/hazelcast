package com.hazelcast.internal.metrics.impl;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;

public class ScanAndWeakRegisterTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test
    public void testGcEventually() {
        metricsRegistry.scanAndWeakRegister(new DummyObject(), "dummy");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                System.gc();
                LongGauge gauge = metricsRegistry.newLongGauge("dummy.value");
                assertEquals(0, gauge.read());
            }
        });
    }

    @Test
    public void test() {
        DummyObject source = new DummyObject();
        metricsRegistry.scanAndWeakRegister(source, "dummy");
        LongGauge gauge = metricsRegistry.newLongGauge("dummy.value");
        assertEquals(100, gauge.read());
    }

    static class DummyObject {
        @Probe
        int value = 100;
    }
}
