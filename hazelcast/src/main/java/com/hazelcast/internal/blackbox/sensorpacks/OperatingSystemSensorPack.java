/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.blackbox.sensorpacks;

import com.hazelcast.internal.blackbox.Blackbox;
import com.hazelcast.internal.blackbox.DoubleSensorInput;
import com.hazelcast.internal.blackbox.LongSensorInput;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * A Sensor pack for exposing {@link java.lang.management.OperatingSystemMXBean} sensors.
 */
public final class OperatingSystemSensorPack {

    private static final double PERCENTAGE_MULTIPLIER = 100d;

    private OperatingSystemSensorPack() {
    }

    /**
     * Registers all the sensors in this sensor pack.
     *
     * @param blackbox the Blackbox the sensors are registered on.
     */
    public static void register(Blackbox blackbox) {
        checkNotNull(blackbox, "blackbox");

        OperatingSystemMXBean mxBean = ManagementFactory.getOperatingSystemMXBean();

        registerMethod(blackbox, mxBean, "getCommittedVirtualMemorySize", "os.committedVirtualMemorySize");
        registerMethod(blackbox, mxBean, "getFreePhysicalMemorySize", "os.freePhysicalMemorySize");
        registerMethod(blackbox, mxBean, "getFreeSwapSpaceSize", "os.freeSwapSpaceSize");
        registerMethod(blackbox, mxBean, "getProcessCpuTime", "os.processCpuTime");
        registerMethod(blackbox, mxBean, "getTotalPhysicalMemorySize", "os.totalPhysicalMemorySize");
        registerMethod(blackbox, mxBean, "getTotalSwapSpaceSize", "os.totalSwapSpaceSize");
        registerMethod(blackbox, mxBean, "getMaxFileDescriptorCount", "os.maxFileDescriptorCount");
        registerMethod(blackbox, mxBean, "getOpenFileDescriptorCount", "os.openFileDescriptorCount");
        registerMethod(blackbox, mxBean, "getProcessCpuLoad", "os.processCpuLoad");
        registerMethod(blackbox, mxBean, "getSystemCpuLoad", "os.systemCpuLoad");

        blackbox.register(mxBean, "os.systemLoadAverage",
                new DoubleSensorInput<OperatingSystemMXBean>() {
                    @Override
                    public double get(OperatingSystemMXBean bean) {
                        return PERCENTAGE_MULTIPLIER * bean.getSystemLoadAverage();
                    }
                }
        );
    }


    // This method doesn't depend on the OperatingSystemMXBean so it can be tested. Due to not knowing
    // the exact OperatingSystemMXBean class it is very difficult to get this class tested.
    static void registerMethod(Blackbox blackBox, Object osBean, String methodName, String parameter) {
        final Method method = getMethod(osBean, methodName);

        if (method == null) {
            return;
        }

        if (long.class.equals(method.getReturnType())) {
            blackBox.register(osBean, parameter,
                    new LongSensorInput() {
                        @Override
                        public long get(Object bean) throws Exception {
                            return (Long) method.invoke(bean);
                        }
                    });
        } else {
            blackBox.register(osBean, parameter,
                    new DoubleSensorInput() {
                        @Override
                        public double get(Object bean) throws Exception {
                            return (Double) method.invoke(bean);
                        }
                    });
        }
    }

    private static Method getMethod(Object source, String methodName) {
        try {
            Method method = source.getClass().getDeclaredMethod(methodName);
            method.setAccessible(true);
            return method;
        } catch (Exception e) {
            return null;
        }
    }
}
