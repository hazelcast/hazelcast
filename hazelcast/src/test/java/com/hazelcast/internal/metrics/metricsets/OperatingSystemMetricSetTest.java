package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.Metric;
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

import static com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricsSet.registerMethod;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// The operating system sensor pack is very hard to test because you never know the
// concrete implementation of the OperatingSystemMXBean.
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class OperatingSystemMetricSetTest extends HazelcastTestSupport {

    private MetricsRegistryImpl blackbox;

    @Before
    public void setup() {
        blackbox = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void utilityConstructor(){
        assertUtilityConstructor(OperatingSystemMetricsSet.class);
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
        boolean contains = blackbox.getNames().contains(parameter);
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
        registerMethod(blackbox, fakeOperatingSystemBean, "doubleMethod", "doubleMethod");

        Gauge gauge = blackbox.getGauge("doubleMethod");
        assertEquals(fakeOperatingSystemBean.doubleMethod(), gauge.readDouble(), 0.1);
    }

    @Test
    public void registerMethod_whenLong() {
        blackbox = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(blackbox, fakeOperatingSystemBean, "longMethod", "longMethod");

        Gauge gauge = blackbox.getGauge("longMethod");
        assertEquals(fakeOperatingSystemBean.longMethod(), gauge.readLong());
    }

    @Test
    public void registerMethod_whenNotExist() {
        blackbox = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(blackbox, fakeOperatingSystemBean, "notexist", "notexist");

        boolean parameterExist = blackbox.getNames().contains("notexist");
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
