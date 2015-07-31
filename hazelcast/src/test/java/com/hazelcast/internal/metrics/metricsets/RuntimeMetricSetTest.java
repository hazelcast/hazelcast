package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.Metric;
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
        final Gauge gauge = blackbox.getGauge("runtime.freeMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.freeMemory(), gauge.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void totalMemory() {
        final Gauge gauge = blackbox.getGauge("runtime.totalMemory");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.totalMemory(), gauge.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void maxMemory() {
        final Gauge gauge = blackbox.getGauge("runtime.maxMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(runtime.maxMemory(), gauge.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void usedMemory() {
        final Gauge gauge = blackbox.getGauge("runtime.usedMemory");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = runtime.totalMemory() - runtime.freeMemory();
                assertEquals(expected, gauge.readLong(), TEN_MB);
            }
        });
    }

    @Test
    public void availableProcessors() {
        Gauge gauge = blackbox.getGauge("runtime.availableProcessors");
        assertEquals(runtime.availableProcessors(), gauge.readLong());
    }

    @Test
    public void uptime() {
        final Gauge gauge = blackbox.getGauge("runtime.uptime");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                double expected = ManagementFactory.getRuntimeMXBean().getUptime();
                assertEquals(expected, gauge.readLong(), TimeUnit.MINUTES.toMillis(1));
            }
        });
    }
}
