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
import java.lang.management.ThreadMXBean;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ThreadMetricSetTest extends HazelcastTestSupport {

    private static final ThreadMXBean MX_BEAN = ManagementFactory.getThreadMXBean();

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(ThreadMetricSet.class);
    }

    @Test
    public void threadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.threadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getThreadCount(), gauge.read(), 10);
            }
        });
    }

    @Test
    public void peakThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.peakThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getPeakThreadCount(), gauge.read(), 10);
            }
        });
    }

    @Test
    public void daemonThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.daemonThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getDaemonThreadCount(), gauge.read(), 10);
            }
        });
    }

    @Test
    public void totalStartedThreadCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("thread.totalStartedThreadCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(MX_BEAN.getTotalStartedThreadCount(), gauge.read(), 10);
            }
        });
    }
}
