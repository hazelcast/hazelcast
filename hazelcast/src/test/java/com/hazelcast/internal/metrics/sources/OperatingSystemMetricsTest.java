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

package com.hazelcast.internal.metrics.sources;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static org.junit.Assume.assumeTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;

import org.junit.AssumptionViolatedException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.internal.metrics.AbstractMetricsTest;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.ProbeUtils;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class OperatingSystemMetricsTest extends AbstractMetricsTest {

    private final MetricsSource OS = new MetricsSource() {

        @Override
        public void collectAll(CollectionCycle cycle) {
            cycle.collect(MANDATORY, new FakeOperatingSystemBean(),
                    new String[] { "longMethod", "doubleMethod", "notExist" });
        }
    };

    @Before
    public void setup() {
        registry.register(new MachineMetrics());
        registry.register(OS);
    }

    @Test
    public void testGenericSensors() {
        assertContainsSensor("os.systemLoadAverage");
    }

    @Test
    public void testComSunManagementUnixOperatingSystemMXBean() {
        assumeOperatingSystemMXBeanType("com.sun.management.UnixOperatingSystemMXBean");
        assertContainsSensor("os.maxFileDescriptorCount");
        assertContainsSensor("os.openFileDescriptorCount");

    }

    @Test
    public void testComSunManagementOperatingSystemMXBean() {
        assumeOperatingSystemMXBeanType("com.sun.management.OperatingSystemMXBean");

        assertContainsSensor("os.committedVirtualMemorySize");
        assertContainsSensor("os.freePhysicalMemorySize");
        assertContainsSensor("os.freeSwapSpaceSize");
        assertContainsSensor("os.processCpuTime");
        assertContainsSensor("os.totalPhysicalMemorySize");
        assertContainsSensor("os.totalSwapSpaceSize");

        assumeThatNoJDK6();
        // only available in JDK 7+
        assertContainsSensor("os.processCpuLoad");
        assertContainsSensor("os.systemCpuLoad");
    }

    private void assertContainsSensor(String expectedKey) {
        assertProbed(expectedKey);
    }

    private void assumeOperatingSystemMXBeanType(String expected) {
        OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        try {
            Class<?> expectedInterface = Class.forName(expected);
            assumeTrue(expectedInterface.isAssignableFrom(bean.getClass()));
        } catch (ClassNotFoundException e) {
            throw new AssumptionViolatedException("MXBean interface " + expected + " was not found");
        }
    }

    @Test
    public void registerMethod_whenDouble() {
        assertProbed("doubleMethod", ProbeUtils.toLong(10d));
    }

    @Test
    public void registerMethod_whenLong() {
        assertProbed("longMethod", 10L);
    }

    @Test
    public void registerMethod_whenNotExist() {
        assertNotProbed("notExist");
    }

    public static class FakeOperatingSystemBean {

        public double doubleMethod() {
            return 10d;
        }

        public long longMethod() {
            return 10;
        }
    }
}
