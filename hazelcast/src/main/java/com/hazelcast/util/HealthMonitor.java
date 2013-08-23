/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.logging.Level;

import static java.lang.String.format;

//http://blog.scoutapp.com/articles/2009/07/31/understanding-load-averages
//http://docs.oracle.com/javase/7/docs/jre/api/management/extension/com/sun/management/OperatingSystemMXBean.html
public class HealthMonitor extends Thread {
    private final ILogger logger;
    private final Node node;
    private final Runtime runtime;
    private final OperatingSystemMXBean osMxBean;
    private final HealthMonitorLevel logLevel;
    private double treshold = 70;

    public HealthMonitor(Node node, HealthMonitorLevel logLevel) {
        super(node.threadGroup, node.getThreadNamePrefix("HealthMonitor"));
        setDaemon(true);
        this.node = node;
        this.logger = node.getLogger(HealthMonitor.class.getName());
        this.runtime = Runtime.getRuntime();
        this.osMxBean = ManagementFactory.getOperatingSystemMXBean();
        this.logLevel = logLevel;
    }

    public void run() {
        while (node.isActive()) {
            HealthMetrics metrics;
            switch (logLevel) {
                case OFF:
                    break;
                case NOISY:
                    metrics = new HealthMetrics();
                    logger.log(Level.INFO, metrics.toString());
                    break;
                case SILENT:
                    metrics = new HealthMetrics();
                    if (metrics.exceedsTreshold()) {
                        logger.log(Level.INFO, metrics.toString());
                    }
                    break;
                default:
                    throw new IllegalStateException("unrecognized logLevel:" + logLevel);
            }

            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    public class HealthMetrics {
        long memoryFree;
        long memoryTotal;
        long memoryUsed;
        long memoryMax;
        double memoryUsedOfTotalPercentage;
        double memoryUsedOfMaxPercentage;
        //following three load variables are always between 0 and 100.
        double processCpuLoad;
        double systemLoadAverage;
        double systemCpuLoad;

        public HealthMetrics() {
            memoryFree = runtime.freeMemory();
            memoryTotal = runtime.totalMemory();
            memoryUsed = memoryTotal - memoryFree;
            memoryMax = runtime.maxMemory();
            memoryUsedOfTotalPercentage = 100d * memoryUsed / memoryTotal;
            memoryUsedOfMaxPercentage = 100d * memoryUsed / memoryMax;
            processCpuLoad = get(osMxBean, "getProcessCpuLoad", -1L);
            systemLoadAverage = get(osMxBean, "getSystemLoadAverage", -1L);
            systemCpuLoad = get(osMxBean, "getSystemCpuLoad", -1L);
        }

        public boolean exceedsTreshold() {
            if (memoryUsedOfMaxPercentage > treshold) {
                return true;
            }

            if (processCpuLoad > treshold) {
                return true;
            }

            if (systemCpuLoad > treshold) {
                return true;
            }

            if (systemCpuLoad > treshold) {
                return true;
            }

            return false;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("\nmemory ");
            sb.append("used=").append(bytesToString(memoryUsed)).append(", ");
            sb.append("free=").append(bytesToString(memoryFree)).append(", ");
            sb.append("total=").append(bytesToString(memoryTotal)).append(", ");
            sb.append("max=").append(bytesToString(memoryMax)).append(", ");
            sb.append("used/total=").append(percentageString(memoryUsedOfTotalPercentage)).append(" ");
            sb.append("used/max=").append(percentageString(memoryUsedOfMaxPercentage));
            sb.append("\n");
            sb.append("cpu ");
            sb.append("process-load=").append(format("%.2f", processCpuLoad)).append("%, ");
            sb.append("system-load=").append(format("%.2f", systemCpuLoad)).append("%, ");
            sb.append("system-loadaverage=").append(format("%.2f", systemLoadAverage)).append("%");
            return sb.toString();
        }
    }

    private static final String[] UNITS = new String[]{"", "K", "M", "G", "T", "P", "E"};

    private static Long get(OperatingSystemMXBean mbean, String methodName, Long defaultValue) {
        try {
            Method method = mbean.getClass().getMethod(methodName);
            method.setAccessible(true);

            Object value = method.invoke(mbean);
            if (value == null) {
                return defaultValue;
            }

            if (value instanceof Integer) {
                return (long) (Integer) value;
            }

            if (value instanceof Double) {
                double v = (Double) value;
                return Math.round(v * 100);
            }

            if (value instanceof Long) {
                return (Long) value;
            }

            return defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static String percentageString(double p) {
        return format("%.2f", p) + "%";
    }

    public static String bytesToString(long bytes) {
        for (int i = 6; i > 0; i--) {
            double step = Math.pow(1024, i);
            if (bytes > step) return format("%3.1f%s", bytes / step, UNITS[i]);
        }
        return Long.toString(bytes);
    }
}
