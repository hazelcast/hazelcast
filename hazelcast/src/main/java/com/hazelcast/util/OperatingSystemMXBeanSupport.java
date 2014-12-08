/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.util;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

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
     * @param attributeName name
     * @param defaultValue default value
     * @return value of attribute
     */
    public static long readLongAttribute(String attributeName, long defaultValue) {
        try {
            String methodName = "get" + attributeName;
            OperatingSystemMXBean mbean = OPERATING_SYSTEM_MX_BEAN;
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);

            Object value = method.invoke(mbean);
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
            EmptyStatement.ignore(ignored);
        }
        return defaultValue;
    }
}
