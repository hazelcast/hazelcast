/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet.registerMethod;
import static com.hazelcast.logging.Logger.getLogger;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperatingSystemMetricSetTest extends HazelcastTestSupport {

    private MetricsRegistryImpl metricsRegistry;

    @Before
    public void setup() {
        metricsRegistry = new MetricsRegistryImpl(getLogger(MetricsRegistryImpl.class), INFO);
        OperatingSystemMetricSet.register(metricsRegistry);
    }

    @Test
    public void utilityConstructor() {
        assertUtilityConstructor(OperatingSystemMetricSet.class);
    }

    @Test
    public void testGenericSensors() {
        assertContainsSensor("os.systemLoadAverage");
    }

    @Test
    public void testComSunManagementUnixOperatingSystem() {
        assumeOperatingSystemMXBean("com.sun.management.UnixOperatingSystem");

        assertContainsSensor("os.committedVirtualMemorySize");
        assertContainsSensor("os.freePhysicalMemorySize");
        assertContainsSensor("os.freeSwapSpaceSize");
        assertContainsSensor("os.processCpuTime");
        assertContainsSensor("os.totalPhysicalMemorySize");
        assertContainsSensor("os.totalSwapSpaceSize");
        assertContainsSensor("os.maxFileDescriptorCount");
        assertContainsSensor("os.openFileDescriptorCount");

        assumeThatNoJDK6();
        // only available in JDK 7+
        assertContainsSensor("os.processCpuLoad");
        assertContainsSensor("os.systemCpuLoad");
    }

    @Test
    public void testSunManagementOperatingSystemImpl() {
        assumeOperatingSystemMXBean("sun.management.OperatingSystemImpl");

        assertContainsSensor("os.committedVirtualMemorySize");
        assertContainsSensor("os.freePhysicalMemorySize");
        assertContainsSensor("os.freeSwapSpaceSize");
        assertContainsSensor("os.processCpuTime");
        assertContainsSensor("os.totalPhysicalMemorySize");
        assertContainsSensor("os.totalSwapSpaceSize");
        assertContainsSensor("os.processCpuLoad");
        assertContainsSensor("os.systemCpuLoad");

        assumeThatNoWindowsOS();
        // not implemented on Windows
        assertContainsSensor("os.maxFileDescriptorCount");
        assertContainsSensor("os.openFileDescriptorCount");
    }

    private void assertContainsSensor(String parameter) {
        boolean contains = metricsRegistry.getNames().contains(parameter);
        assertTrue("sensor: " + parameter + " is not found", contains);
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
        metricsRegistry = new MetricsRegistryImpl(getLogger(MetricsRegistryImpl.class), INFO);

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(metricsRegistry, fakeOperatingSystemBean, "longMethod", "longMethod");

        LongGauge gauge = metricsRegistry.newLongGauge("longMethod");
        assertEquals(fakeOperatingSystemBean.longMethod(), gauge.read());
    }

    @Test
    public void registerMethod_whenNotExist() {
        metricsRegistry = new MetricsRegistryImpl(getLogger(MetricsRegistryImpl.class), INFO);

        FakeOperatingSystemBean fakeOperatingSystemBean = new FakeOperatingSystemBean();
        registerMethod(metricsRegistry, fakeOperatingSystemBean, "notExist", "notExist");

        boolean parameterExist = metricsRegistry.getNames().contains("notExist");
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
