package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RuntimeMetricSetTest extends HazelcastTestSupport {

    private final static int TEN_MB = 10 * 1024 * 1024;

    private MetricsRegistryImpl blackbox;
    private Runtime runtime;

    @Before
    public void setup() {
        blackbox = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
        runtime = Runtime.getRuntime();
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(RuntimeMetricSet.class);
    }

    @Test
    public void freeMemory() {
        final LongGauge gauge = blackbox.newLongGauge("runtime.freeMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.freeMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void totalMemory() {
        final LongGauge gauge = blackbox.newLongGauge("runtime.totalMemory");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.totalMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void maxMemory() {
        final LongGauge gauge = blackbox.newLongGauge("runtime.maxMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.maxMemory(), gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void usedMemory() {
        final LongGauge gauge = blackbox.newLongGauge("runtime.usedMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = runtime.totalMemory() - runtime.freeMemory();
                assertEquals(expected, gauge.read(), TEN_MB);
            }
        });
    }

    @Test
    public void availableProcessors() {
        LongGauge gauge = blackbox.newLongGauge("runtime.availableProcessors");
        assertEquals(runtime.availableProcessors(), gauge.read());
    }

    @Test
    public void uptime() {
        final LongGauge gauge = blackbox.newLongGauge("runtime.uptime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = ManagementFactory.getRuntimeMXBean().getUptime();
                assertEquals(expected, gauge.read(), TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}
