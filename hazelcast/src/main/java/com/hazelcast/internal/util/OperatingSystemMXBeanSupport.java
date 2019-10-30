/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import static com.hazelcast.internal.util.EmptyStatement.ignore;

/**
 * Support class for reading attributes from OperatingSystemMXBean.
 */
public final class OperatingSystemMXBeanSupport {

    private static final OperatingSystemMXBean OPERATING_SYSTEM_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();
    private static final double PERCENTAGE_MULTIPLIER = 100d;

    private OperatingSystemMXBeanSupport() {
    }

    /**
     * Reads a long attribute from OperatingSystemMXBean.
     *
     * @param attributeName name of the attribute
     * @param defaultValue  default value if the attribute value is null
     * @return value of the attribute
     */
    public static long readLongAttribute(String attributeName, long defaultValue) {
        try {
            String methodName = "get" + attributeName;
            OperatingSystemMXBean systemMXBean = OPERATING_SYSTEM_MX_BEAN;
            Method method = systemMXBean.getClass().getMethod(methodName);
            try {
                method.setAccessible(true);
            } catch (Exception e) {
                return defaultValue;
            }

            Object value = method.invoke(systemMXBean);
            if (value == null) {
                return defaultValue;
            }

            if (value instanceof Long) {
                return (Long) value;
            }

            if (value instanceof Double) {
                double v = (Double) value;
                return Math.round(v * PERCENTAGE_MULTIPLIER);
            }

            if (value instanceof Number) {
                return ((Number) value).longValue();
            }

        } catch (RuntimeException re) {
            throw re;
        } catch (Exception ignored) {
            ignore(ignored);
        }
        return defaultValue;
    }

    /**
     * Reads the system load average attribute from OperatingSystemMXBean.
     *
     * @return system load average or negative value if metric is not available
     */
    public static double getSystemLoadAverage() {
        return OPERATING_SYSTEM_MX_BEAN.getSystemLoadAverage();
    }
}
