package com.hazelcast.internal.metrics.metricsets;

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
        metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(MetricsRegistryImpl.class));
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(OperatingSystemMetricsSet.class);
    }

    @Test
    public void testComSunManagementUnixOperatingSystem() {
        for (String s : metricsRegistry.getNames()) {
            System.out.println(s);
        }


        assertContainsProbe("os.systemLoadAverage");

        assumeOperatingSystemMXBean("com.sun.management.UnixOperatingSystem");

        assertContainsProbe("os.committedVirtualMemorySize");
        assertContainsProbe("os.freePhysicalMemorySize");
        assertContainsProbe("os.freeSwapSpaceSize");
        assertContainsProbe("os.processCpuTime");
        assertContainsProbe("os.totalPhysicalMemorySize");
        assertContainsProbe("os.totalSwapSpaceSize");
        assertContainsProbe("os.maxFileDescriptorCount");
        assertContainsProbe("os.openFileDescriptorCount");

        //only available on java 7+
        //assertContainsSensor("os.processCpuLoad");
        //assertContainsSensor("os.systemCpuLoad");
    }

    @Test
    public void testSunManagementOperatingSystemImpl() {
        assertContainsProbe("os.systemLoadAverage");

        assumeOperatingSystemMXBean("sun.management.OperatingSystemImpl");

        assertContainsProbe("os.committedVirtualMemorySize");
        assertContainsProbe("os.freePhysicalMemorySize");
        assertContainsProbe("os.freeSwapSpaceSize");
        assertContainsProbe("os.processCpuTime");
        assertContainsProbe("os.totalPhysicalMemorySize");
        assertContainsProbe("os.totalSwapSpaceSize");
        assertContainsProbe("os.maxFileDescriptorCount");
        assertContainsProbe("os.openFileDescriptorCount");

        assertContainsProbe("os.processCpuLoad");
        assertContainsProbe("os.systemCpuLoad");
    }

    private void assertContainsProbe(String probeName) {
        boolean contains = metricsRegistry.getNames().contains(probeName);
        assertTrue("sensor:" + probeName + " is not found", contains);
    }

    private void assumeOperatingSystemMXBean(String expected) {
        OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();

        String foundClass = bean.getClass().getName();

        assumeTrue(foundClass + " is not usable", expected.equals(foundClass));
    }
}
