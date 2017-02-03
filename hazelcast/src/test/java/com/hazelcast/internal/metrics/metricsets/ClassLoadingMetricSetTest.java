package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClassLoadingMetricSetTest extends HazelcastTestSupport {

    private static final ClassLoadingMXBean BEAN = ManagementFactory.getClassLoadingMXBean();

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        ILogger logger = mock(ILogger.class);
        HazelcastThreadGroup hazelcastThreadGroup = new HazelcastThreadGroup("name", logger, null);
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO, hazelcastThreadGroup);
        ClassLoadingMetricSet.register(metricsRegistry);
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(ClassLoadingMetricSet.class);
    }

    @Test
    public void loadedClassesCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("classloading.loadedClassesCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getLoadedClassCount(), gauge.read(), 100);
            }
        });
    }

    @Test
    public void totalLoadedClassesCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("classloading.totalLoadedClassesCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getTotalLoadedClassCount(), gauge.read(), 100);
            }
        });
    }

    @Test
    public void unloadedClassCount() {
        final LongGauge gauge = metricsRegistry.newLongGauge("classloading.unloadedClassCount");

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(BEAN.getUnloadedClassCount(), gauge.read(), 100);
            }
        });
    }

}
