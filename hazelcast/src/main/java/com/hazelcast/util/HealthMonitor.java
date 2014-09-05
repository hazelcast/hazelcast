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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.ProxyService;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static java.lang.String.format;

/**
 * http://blog.scoutapp.com/articles/2009/07/31/understanding-load-averages
 * http://docs.oracle.com/javase/7/docs/jre/api/management/extension/com/sun/management/OperatingSystemMXBean.html
 * <p/>
 * Health monitor periodically prints logs about related internal metrics under when hazelcast is under load.
 * Under load means that memory usage is above threshold percentage or process/cpu load is above threshold.
 * <p/>
 * Health monitor can be configured with system properties
 * <p/>
 * "hazelcast.health.monitoring.level"
 * This property can be one of the following
 * NOISY           => does not check threshold , always prints.
 * SILENT(default) => prints only if metrics are above threshold.
 * OFF             => Does not print anything.
 * <p/>
 * "hazelcast.health.monitoring.delay.seconds"
 * Time between printing two logs of health monitor. Default values is 30 seconds
 */

public class HealthMonitor extends Thread {

    private static final String[] UNITS = new String[]{"", "K", "M", "G", "T", "P", "E"};
    private static final double PERCENTAGE_MULTIPLIER = 100d;
    private static final double THRESHOLD = 70;
    private static final Set<String> YOUNG_GC = new HashSet<String>(3);
    private static final Set<String> OLD_GC = new HashSet<String>(3);

    static {
        YOUNG_GC.add("PS Scavenge");
        YOUNG_GC.add("ParNew");
        YOUNG_GC.add("G1 Young Generation");

        OLD_GC.add("PS MarkSweep");
        OLD_GC.add("ConcurrentMarkSweep");
        OLD_GC.add("G1 Old Generation");
    }

    private final ILogger logger;
    private final Node node;
    private final Runtime runtime;
    private final OperatingSystemMXBean osMxBean;
    private final HealthMonitorLevel logLevel;
    private final int delaySeconds;
    private final ExecutionService executionService;
    private final EventService eventService;
    private final OperationService operationService;
    private final ProxyService proxyService;
    private final ConnectionManager connectionManager;
    private final ClientEngineImpl clientEngine;
    private final ThreadMXBean threadMxBean;

    public HealthMonitor(HazelcastInstanceImpl hazelcastInstance, HealthMonitorLevel logLevel, int delaySeconds) {
        super(hazelcastInstance.node.threadGroup, hazelcastInstance.node.getThreadNamePrefix("HealthMonitor"));
        setDaemon(true);

        this.node = hazelcastInstance.node;
        this.logger = node.getLogger(HealthMonitor.class.getName());
        this.runtime = Runtime.getRuntime();
        this.osMxBean = ManagementFactory.getOperatingSystemMXBean();
        this.logLevel = logLevel;
        this.delaySeconds = delaySeconds;
        this.threadMxBean = ManagementFactory.getThreadMXBean();
        this.executionService = node.nodeEngine.getExecutionService();
        this.eventService = node.nodeEngine.getEventService();
        this.operationService = node.nodeEngine.getOperationService();
        this.proxyService = node.nodeEngine.getProxyService();
        this.clientEngine = node.clientEngine;
        this.connectionManager = node.connectionManager;
    }

    @Override
    public void run() {
        if (logLevel == HealthMonitorLevel.OFF) {
            return;
        }

        while (node.isActive()) {
            HealthMetrics metrics;
            switch (logLevel) {
                case NOISY:
                    metrics = new HealthMetrics();
                    logger.log(Level.INFO, metrics.toString());
                    break;
                case SILENT:
                    metrics = new HealthMetrics();
                    if (metrics.exceedsThreshold()) {
                        logger.log(Level.INFO, metrics.toString());
                    }
                    break;
                default:
                    throw new IllegalStateException("unrecognized logLevel:" + logLevel);
            }

            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(delaySeconds));
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    /**
     * Health metrics to be logged under load.
     */
    private class HealthMetrics {
        private final long physicalMemoryTotal;
        private final long physicalMemoryFree;
        private final long swapSpaceTotal;
        private final long swapSpaceFree;
        private final long memoryFree;
        private final long memoryTotal;
        private final long memoryUsed;
        private final long memoryMax;
        private final double memoryUsedOfTotalPercentage;
        private final double memoryUsedOfMaxPercentage;
        //following three load variables are always between 0 and 100.
        private final double processCpuLoad;
        private final double systemLoadAverage;
        private final double systemCpuLoad;
        private final int threadCount;
        private final int peakThreadCount;
        private final int asyncExecutorQueueSize;
        private final int clientExecutorQueueSize;
        private final int queryExecutorQueueSize;
        private final int scheduledExecutorQueueSize;
        private final int systemExecutorQueueSize;
        private final int eventQueueSize;
        private final int operationServiceOperationExecutorQueueSize;
        private final int operationServiceOperationPriorityExecutorQueueSize;
        private final int operationServiceOperationResponseQueueSize;
        private final int runningOperationsCount;
        private final int remoteOperationsCount;
        private final int proxyCount;
        private final int clientEndpointCount;
        private final int activeConnectionCount;
        private final int connectionCount;
        private final int ioExecutorQueueSize;
        private final GcMetrics gcMetrics;

        //CHECKSTYLE:OFF
        public HealthMetrics() {
            physicalMemoryTotal = get(osMxBean, "getTotalPhysicalMemorySize", -1L);
            physicalMemoryFree = get(osMxBean, "getFreePhysicalMemorySize", -1L);
            swapSpaceTotal = get(osMxBean, "getTotalSwapSpaceSize", -1L);
            swapSpaceFree = get(osMxBean, "getFreeSwapSpaceSize", -1L);
            memoryFree = runtime.freeMemory();
            memoryTotal = runtime.totalMemory();
            memoryUsed = memoryTotal - memoryFree;
            memoryMax = runtime.maxMemory();
            memoryUsedOfTotalPercentage = PERCENTAGE_MULTIPLIER * memoryUsed / memoryTotal;
            memoryUsedOfMaxPercentage = PERCENTAGE_MULTIPLIER * memoryUsed / memoryMax;
            processCpuLoad = get(osMxBean, "getProcessCpuLoad", -1L);
            systemLoadAverage = get(osMxBean, "getSystemLoadAverage", -1L);
            systemCpuLoad = get(osMxBean, "getSystemCpuLoad", -1L);
            threadCount = threadMxBean.getThreadCount();
            peakThreadCount = threadMxBean.getPeakThreadCount();
            asyncExecutorQueueSize = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR).getQueueSize();
            clientExecutorQueueSize = executionService.getExecutor(ExecutionService.CLIENT_EXECUTOR).getQueueSize();
            queryExecutorQueueSize = executionService.getExecutor(ExecutionService.QUERY_EXECUTOR).getQueueSize();
            scheduledExecutorQueueSize = executionService.getExecutor(ExecutionService.SCHEDULED_EXECUTOR).getQueueSize();
            systemExecutorQueueSize = executionService.getExecutor(ExecutionService.SYSTEM_EXECUTOR).getQueueSize();
            ioExecutorQueueSize = executionService.getExecutor(ExecutionService.IO_EXECUTOR).getQueueSize();
            eventQueueSize = eventService.getEventQueueSize();
            operationServiceOperationExecutorQueueSize = operationService.getOperationExecutorQueueSize();
            operationServiceOperationPriorityExecutorQueueSize = operationService.getPriorityOperationExecutorQueueSize();
            operationServiceOperationResponseQueueSize = operationService.getResponseQueueSize();
            runningOperationsCount = operationService.getRunningOperationsCount();
            remoteOperationsCount = operationService.getRemoteOperationsCount();
            proxyCount = proxyService.getProxyCount();
            clientEndpointCount = clientEngine.getClientEndpointCount();
            activeConnectionCount = connectionManager.getActiveConnectionCount();
            connectionCount = connectionManager.getConnectionCount();
            gcMetrics = new GcMetrics();
        }
        //CHECKSTYLE:ON

        public boolean exceedsThreshold() {
            if (memoryUsedOfMaxPercentage > THRESHOLD) {
                return true;
            }

            if (processCpuLoad > THRESHOLD) {
                return true;
            }

            if (systemCpuLoad > THRESHOLD) {
                return true;
            }

            return false;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("processors=").append(runtime.availableProcessors()).append(", ");
            sb.append("physical.memory.total=").append(numberToUnitRepresentation(physicalMemoryTotal)).append(", ");
            sb.append("physical.memory.free=").append(numberToUnitRepresentation(physicalMemoryFree)).append(", ");
            sb.append("swap.space.total=").append(numberToUnitRepresentation(swapSpaceTotal)).append(", ");
            sb.append("swap.space.free=").append(numberToUnitRepresentation(swapSpaceFree)).append(", ");
            sb.append("heap.memory.used=").append(numberToUnitRepresentation(memoryUsed)).append(", ");
            sb.append("heap.memory.free=").append(numberToUnitRepresentation(memoryFree)).append(", ");
            sb.append("heap.memory.total=").append(numberToUnitRepresentation(memoryTotal)).append(", ");
            sb.append("heap.memory.max=").append(numberToUnitRepresentation(memoryMax)).append(", ");
            sb.append("heap.memory.used/total=").append(percentageString(memoryUsedOfTotalPercentage)).append(", ");
            sb.append("heap.memory.used/max=").append(percentageString(memoryUsedOfMaxPercentage)).append((", "));
            sb.append("minor.gc.count=").append(gcMetrics.minorCount).append(", ");
            sb.append("minor.gc.time=").append(gcMetrics.minorTime).append("ms, ");
            sb.append("major.gc.count=").append(gcMetrics.majorCount).append(", ");
            sb.append("major.gc.time=").append(gcMetrics.majorTime).append("ms, ");
            if (gcMetrics.unknownCount > 0) {
                sb.append("unknown.gc.count=").append(gcMetrics.unknownCount).append(", ");
                sb.append("unknown.gc.time=").append(gcMetrics.unknownTime).append("ms, ");
            }
            sb.append("load.process=").append(format("%.2f", processCpuLoad)).append("%, ");
            sb.append("load.system=").append(format("%.2f", systemCpuLoad)).append("%, ");
            sb.append("load.systemAverage=").append(format("%.2f", systemLoadAverage)).append("%, ");
            sb.append("thread.count=").append(threadCount).append(", ");
            sb.append("thread.peakCount=").append(peakThreadCount).append(", ");
            sb.append("event.q.size=").append(eventQueueSize).append(", ");
            sb.append("executor.q.async.size=").append(asyncExecutorQueueSize).append(", ");
            sb.append("executor.q.client.size=").append(clientExecutorQueueSize).append(", ");
            sb.append("executor.q.query.size=").append(queryExecutorQueueSize).append(", ");
            sb.append("executor.q.scheduled.size=").append(scheduledExecutorQueueSize).append(", ");
            sb.append("executor.q.io.size=").append(ioExecutorQueueSize).append(", ");
            sb.append("executor.q.system.size=").append(systemExecutorQueueSize).append(", ");
            sb.append("executor.q.operation.size=").append(operationServiceOperationExecutorQueueSize).append(", ");
            sb.append("executor.q.priorityOperation.size=").
                    append(operationServiceOperationPriorityExecutorQueueSize).append(", ");
            sb.append("executor.q.response.size=").append(operationServiceOperationResponseQueueSize).append(", ");
            sb.append("operations.remote.size=").append(remoteOperationsCount).append(", ");
            sb.append("operations.running.size=").append(runningOperationsCount).append(", ");
            sb.append("proxy.count=").append(proxyCount).append(", ");
            sb.append("clientEndpoint.count=").append(clientEndpointCount).append(", ");
            sb.append("connection.active.count=").append(activeConnectionCount).append(", ");
            sb.append("connection.count=").append(connectionCount);
            return sb.toString();
        }
    }

    private static final class GcMetrics {
        final long minorCount;
        final long minorTime;
        final long majorCount;
        final long majorTime;
        final long unknownCount;
        final long unknownTime;

        //CHECKSTYLE:OFF
        private GcMetrics() {
            long minorCount = 0;
            long minorTime = 0;
            long majorCount = 0;
            long majorTime = 0;
            long unknownCount = 0;
            long unknownTime = 0;

            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                long count = gc.getCollectionCount();
                if (count >= 0) {
                    if (YOUNG_GC.contains(gc.getName())) {
                        minorCount += count;
                        minorTime += gc.getCollectionTime();
                    } else if (OLD_GC.contains(gc.getName())) {
                        majorCount += count;
                        majorTime += gc.getCollectionTime();
                    } else {
                        unknownCount += count;
                        unknownTime += gc.getCollectionTime();
                    }
                }
            }

            this.minorCount = minorCount;
            this.minorTime = minorTime;
            this.majorCount = majorCount;
            this.majorTime = majorTime;
            this.unknownCount = unknownCount;
            this.unknownTime = unknownTime;
        }
        //CHECKSTYLE:ON
    }

    private static long get(OperatingSystemMXBean mbean, String methodName, long defaultValue) {
        try {
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

    public static String percentageString(double p) {
        return format("%.2f", p) + "%";
    }

    public static String numberToUnitRepresentation(long number) {
        //CHECKSTYLE:OFF
        for (int i = 6; i > 0; i--) {
            double step = Math.pow(1024, i); // 1024 is for 1024 kb is 1 MB etc
            if (number > step) {
                return format("%3.1f%s", number / step, UNITS[i]);
            }
        }
        //CHECKSTYLE:ON
        return Long.toString(number);
    }

}
