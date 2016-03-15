package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet.registerMethod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// The operating system sensor pack is very hard to test because you never know the
// concrete implementation of the OperatingSystemMXBean.
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperatingSystemMetricSetTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(OperatingSystemMetricSet.class);
    }

    @Test
    public void testComSunManagementUnixOperatingSystem() {
        assertContainsSensor("os.systemLoadAverage");

        assumeOperatingSystemMXBean("com.sun.management.UnixOperatingSystem");

        assertContainsSensor("os.committedVirtualMemorySize");
        assertContainsSensor("os.freePhysicalMemorySize");
        assertContainsSensor("os.freeSwapSpaceSize");
        assertContainsSensor("os.processCpuTime");
        assertContainsSensor("os.totalPhysicalMemorySize");
        assertContainsSensor("os.totalSwapSpaceSize");
        assertContainsSensor("os.maxFileDescriptorCount");
        assertContainsSensor("os.openFileDescriptorCount");

        //only available on java 7+
        //assertContainsSensor("os.processCpuLoad");
        //assertContainsSensor("os.systemCpuLoad");
    }

    @Test
    public void testSunManagementOperatingSystemImpl() {
        assertContainsSensor("os.systemLoadAverage");

        assumeOperatingSystemMXBean("sun.management.OperatingSystemImpl");

        assertContainsSensor("os.committedVirtualMemorySize");
        assertContainsSensor("os.freePhysicalMemorySize");
        assertContainsSensor("os.freeSwapSpaceSize");
        assertContainsSensor("os.processCpuTime");
        assertContainsSensor("os.totalPhysicalMemorySize");
        assertContainsSensor("os.totalSwapSpaceSize");
        assertContainsSensor("os.maxFileDescriptorCount");
        assertContainsSensor("os.openFileDescriptorCount");

        assertContainsSensor("os.processCpuLoad");
        assertContainsSensor("os.systemCpuLoad");
    }

    private void assertContainsSensor(String parameter) {
        boolean contains = metricsRegistry.getNames().contains(parameter);
        assertTrue("sensor:" + parameter + " is not found", contains);
    }

    private void assumeOperatingSystemMXBean(String expected) {
        OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

        String foundClass = bean.getClass().getName();

        assumeTrue(foundClass + " is not usable", expected.equals(foundClass));
    }

    @Test
    public void registerMethod_whenDouble() {
        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(metricsRegistry, fakeOperatingSystemBean, "doubleMethod", "doubleMethod");

        DoubleGauge gauge = metricsRegistry.newDoubleGauge("doubleMethod");
        assertEquals(fakeOperatingSystemBean.doubleMethod(), gauge.read(), 0.1);
    }

    @Test
    public void registerMethod_whenLong() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(metricsRegistry, fakeOperatingSystemBean, "longMethod", "longMethod");

        LongGauge gauge = metricsRegistry.newLongGauge("longMethod");
        assertEquals(fakeOperatingSystemBean.longMethod(), gauge.read());
    }

    @Test
    public void registerMethod_whenNotExist() {
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class), INFO);

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(metricsRegistry, fakeOperatingSystemBean, "notexist", "notexist");

        boolean parameterExist = metricsRegistry.getNames().contains("notexist");
        assertFalse(parameterExist);
    }

    public class FakeOperatingSystemBean {
        double doubleMethod() {
            return 10;
        }

        long longMethod() {
            return 10;
        }
    }

}
