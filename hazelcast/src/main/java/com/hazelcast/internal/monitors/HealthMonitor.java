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

package com.hazelcast.internal.monitors;

import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.blackbox.Blackbox;
import com.hazelcast.internal.blackbox.Sensor;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryStats;

import java.util.logging.Level;

import static com.hazelcast.internal.monitors.HealthMonitorLevel.OFF;
import static com.hazelcast.internal.monitors.HealthMonitorLevel.valueOf;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

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
public class HealthMonitor {

    private static final String[] UNITS = new String[]{"", "K", "M", "G", "T", "P", "E"};
    private static final double PERCENTAGE_MULTIPLIER = 100d;
    private static final double THRESHOLD = 70;

    private final ILogger logger;
    private final Node node;
    private final HealthMonitorLevel monitorLevel;
    private final Blackbox blackbox;
    private final HealthMonitorThread monitorThread;

    public HealthMonitor(HazelcastInstanceImpl hazelcastInstance) {
        this.node = hazelcastInstance.node;
        this.logger = node.getLogger(HealthMonitor.class);
        this.blackbox = node.nodeEngine.getBlackbox();
        this.monitorLevel = getHealthMonitorLevel();
        this.monitorThread = initMonitorThread();
    }

    private HealthMonitorThread initMonitorThread() {
        if (monitorLevel == OFF) {
            return null;
        }

        int delaySeconds = node.getGroupProperties().HEALTH_MONITORING_DELAY_SECONDS.getInteger();
        return new HealthMonitorThread(delaySeconds);
    }

    public HealthMonitor start() {
        if (monitorLevel == OFF) {
            logger.finest("HealthMonitor is disabled");
            return this;
        }

        monitorThread.start();
        logger.finest("HealthMonitor started");
        return this;
    }

    private HealthMonitorLevel getHealthMonitorLevel() {
        GroupProperties properties = node.getGroupProperties();
        String healthMonitorLevelString = properties.HEALTH_MONITORING_LEVEL.getString();
        return valueOf(healthMonitorLevelString);
    }

    private final class HealthMonitorThread extends Thread {
        private final int delaySeconds;
        private boolean performanceLogHint;

        private HealthMonitorThread(int delaySeconds) {
            super(node.getHazelcastThreadGroup().getInternalThreadGroup(),
                    node.getHazelcastThreadGroup().getThreadNamePrefix("HealthMonitor"));
            setDaemon(true);
            this.delaySeconds = delaySeconds;
            this.performanceLogHint = node.getGroupProperties().PERFORMANCE_MONITOR_ENABLED.getBoolean();
        }

        @Override
        public void run() {
            HealthMetricsRenderer renderer = new HealthMetricsRenderer();

            try {
                while (node.isActive()) {
                    renderer.init();
                    switch (monitorLevel) {
                        case NOISY:
                            logger.log(Level.INFO, renderer.render());
                            break;
                        case SILENT:
                            if (renderer.exceedsThreshold()) {
                                logPerformanceMonitorHint();
                                logger.log(Level.INFO, renderer.render());
                            }
                            break;
                        default:
                            throw new IllegalStateException("unrecognized HealthMonitorLevel:" + monitorLevel);
                    }

                    try {
                        Thread.sleep(SECONDS.toMillis(delaySeconds));
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable t) {
                logger.warning("Health Monitor failed", t);
            }
        }

        private void logPerformanceMonitorHint() {
            if (!performanceLogHint) {
                return;
            }

            // we only log the hint once.
            performanceLogHint = false;

            logger.info(
                    "The HealthMonitor has detected a high load on the system. For more detailed information, \n"
                            + "enable the PerformanceMonitor by adding -D" + GroupProperties.PROP_PERFORMANCE_MONITOR_ENABLED
                            + "=true");
        }
    }

    private class HealthMetricsRenderer {
        private double memoryUsedOfTotalPercentage;
        private double memoryUsedOfMaxPercentage;

        private final Sensor clientEndpointCount;
        private final Sensor clusterTimeDiff;

        private final Sensor executorAsyncQueueSize;
        private final Sensor executorClientQueueSize;
        private final Sensor executorClusterQueueSize;
        private final Sensor executorScheduledQueueSize;
        private final Sensor executorSystemQueueSize;
        private final Sensor executorIoQueueSize;
        private final Sensor executorQueryQueueSize;
        private final Sensor executorMapLoadQueueSize;
        private final Sensor executorMapLoadAllKeysQueueSize;

        private final Sensor eventQueueSize;

        private final Sensor gcMinorCount;
        private final Sensor gcMinorTime;
        private final Sensor gcMajorCount;
        private final Sensor gcMajorTime;
        private final Sensor gcUnknownCount;
        private final Sensor gcUnknownTime;

        private final Sensor runtimeAvailableProcessors;
        private final Sensor runtimeMaxMemory;
        private final Sensor runtimeFreeMemory;
        private final Sensor runtimeAvailableMemory;
        private final Sensor runtimeTotalMemory;
        private final Sensor runtimeUsedMemory;

        private final Sensor threadPeakThreadCount;
        private final Sensor threadThreadCount;

        private final Sensor osProcessCpuLoad;
        private final Sensor osSystemLoadAverage;
        private final Sensor osSystemCpuLoad;
        private final Sensor osTotalPhysicalMemorySize;
        private final Sensor osFreePhysicalMemorySize;
        private final Sensor osTotalSwapSpaceSize;
        private final Sensor osFreeSwapSpaceSize;

        private final Sensor operationServiceExecutorQueueSize;
        private final Sensor operationServiceExecutorPriorityQueueSize;
        private final Sensor operationServiceResponseQueueSize;
        private final Sensor operationServiceRunningOperationsCount;
        private final Sensor operationServiceCompletedOperationsCount;
        private final Sensor operationServicePendingInvocationsCount;
        private final Sensor operationServicePendingInvocationsPercentage;

        private final Sensor proxyCount;

        private final Sensor tcpConnectionActiveCount;
        private final Sensor tcpConnectionCount;
        private final Sensor tcpConnectionClientCount;

        //CHECKSTYLE:OFF
        public HealthMetricsRenderer() {
            this.clientEndpointCount = blackbox.getSensor("client.endpoint.count");

            this.eventQueueSize = blackbox.getSensor("event.eventQueueSize");

            this.clusterTimeDiff = blackbox.getSensor("cluster.clock.clusterTimeDiff");

            this.executorAsyncQueueSize = blackbox.getSensor("executor.hz:async.queueSize");
            this.executorClientQueueSize = blackbox.getSensor("executor.hz:client.queueSize");
            this.executorClusterQueueSize = blackbox.getSensor("executor.hz:cluster.queueSize");
            this.executorScheduledQueueSize = blackbox.getSensor("executor.hz:scheduled.queueSize");
            this.executorSystemQueueSize = blackbox.getSensor("executor.hz:system.queueSize");
            this.executorIoQueueSize = blackbox.getSensor("executor.hz:io.queueSize");
            this.executorMapLoadQueueSize = blackbox.getSensor("executor.hz:map-load.queueSize");
            this.executorMapLoadAllKeysQueueSize = blackbox.getSensor("executor.hz:map-loadAllKeys.queueSize");
            this.executorQueryQueueSize = blackbox.getSensor("executor.hz:query.queueSize");

            this.gcMinorCount = blackbox.getSensor("gc.minorCount");
            this.gcMinorTime = blackbox.getSensor("gc.minorTime");
            this.gcMajorCount = blackbox.getSensor("gc.majorCount");
            this.gcMajorTime = blackbox.getSensor("gc.majorTime");
            this.gcUnknownCount = blackbox.getSensor("gc.unknownCount");
            this.gcUnknownTime = blackbox.getSensor("gc.unknownTime");

            this.operationServiceExecutorQueueSize = blackbox.getSensor("operation.queue.size");
            this.operationServiceExecutorPriorityQueueSize = blackbox.getSensor("operation.priority-queue.size");
            this.operationServiceResponseQueueSize = blackbox.getSensor("operation.response-queue.size");
            this.operationServiceRunningOperationsCount = blackbox.getSensor("operation.running.count");
            this.operationServiceCompletedOperationsCount = blackbox.getSensor("operation.completed.count");
            this.operationServicePendingInvocationsCount = blackbox.getSensor("operation.invocations.pending");
            this.operationServicePendingInvocationsPercentage = blackbox.getSensor("operation.invocations.used");

            this.osProcessCpuLoad = blackbox.getSensor("os.processCpuLoad");
            this.osSystemLoadAverage = blackbox.getSensor("os.systemLoadAverage");
            this.osSystemCpuLoad = blackbox.getSensor("os.systemCpuLoad");
            this.osTotalPhysicalMemorySize = blackbox.getSensor("os.totalPhysicalMemorySize");
            this.osFreePhysicalMemorySize = blackbox.getSensor("os.freePhysicalMemorySize");
            this.osTotalSwapSpaceSize = blackbox.getSensor("os.totalSwapSpaceSize");
            this.osFreeSwapSpaceSize = blackbox.getSensor("os.freeSwapSpaceSize");

            this.proxyCount = blackbox.getSensor("proxy.proxyCount");

            this.runtimeAvailableProcessors = blackbox.getSensor("runtime.availableProcessors");
            this.runtimeMaxMemory = blackbox.getSensor("runtime.maxMemory");
            this.runtimeFreeMemory = blackbox.getSensor("runtime.freeMemory");
            this.runtimeAvailableMemory = blackbox.getSensor("runtime.availableMemory");
            this.runtimeTotalMemory = blackbox.getSensor("runtime.totalMemory");
            this.runtimeUsedMemory = blackbox.getSensor("runtime.usedMemory");

            this.threadThreadCount = blackbox.getSensor("thread.threadCount");
            this.threadPeakThreadCount = blackbox.getSensor("thread.peakThreadCount");

            this.tcpConnectionActiveCount = blackbox.getSensor("tcp.connection.activeCount");
            this.tcpConnectionCount = blackbox.getSensor("tcp.connection.count");
            this.tcpConnectionClientCount = blackbox.getSensor("tcp.connection.clientCount");
        }

        public void init() {
            memoryUsedOfTotalPercentage = PERCENTAGE_MULTIPLIER * runtimeUsedMemory.readLong() / runtimeTotalMemory.readLong();
            memoryUsedOfMaxPercentage = PERCENTAGE_MULTIPLIER * runtimeUsedMemory.readLong() / runtimeMaxMemory.readLong();
        }

        public boolean exceedsThreshold() {
            if (memoryUsedOfMaxPercentage > THRESHOLD) {
                return true;
            }

            if (osProcessCpuLoad.readDouble() > THRESHOLD) {
                return true;
            }

            if (osSystemCpuLoad.readDouble() > THRESHOLD) {
                return true;
            }

            if (operationServicePendingInvocationsPercentage.readDouble() > THRESHOLD) {
                return true;
            }

            return false;
        }

        public String render() {
            StringBuilder sb = new StringBuilder();
            renderProcessors(sb);
            renderPhysicalMemory(sb);
            renderSwap(sb);
            renderHeap(sb);
            renderNativeMemory(sb);
            renderGc(sb);
            renderLoad(sb);
            renderThread(sb);
            renderCluster(sb);
            renderEvents(sb);
            renderExecutors(sb);
            renderOperationService(sb);
            renderProxy(sb);
            renderClient(sb);
            renderConnection(sb);
            return sb.toString();
        }

        private void renderConnection(StringBuilder sb) {
            sb.append("connection.active.count=")
                    .append(tcpConnectionActiveCount.readLong()).append(", ");
            sb.append("client.connection.count=")
                    .append(tcpConnectionClientCount.readLong()).append(", ");
            sb.append("connection.count=")
                    .append(tcpConnectionCount.readLong());
        }

        private void renderClient(StringBuilder sb) {
            sb.append("clientEndpoint.count=")
                    .append(clientEndpointCount.readLong()).append(", ");
        }

        private void renderProxy(StringBuilder sb) {
            sb.append("proxy.count=")
                    .append(proxyCount.readLong()).append(", ");
        }

        private void renderLoad(StringBuilder sb) {
            sb.append(osProcessCpuLoad.getParameter()).append('=')
                    .append(format("%.2f", osProcessCpuLoad.readDouble())).append("%, ");
            sb.append(osSystemCpuLoad.getParameter()).append('=')
                    .append(format("%.2f", osSystemCpuLoad.readDouble())).append("%, ");
            sb.append(osSystemLoadAverage.getParameter()).append('=')
                    .append(format("%.2f", osSystemLoadAverage.readDouble())).append("%, ");
        }

        private void renderProcessors(StringBuilder sb) {
            sb.append("processors=")
                    .append(runtimeAvailableProcessors.readLong()).append(", ");
        }

        private void renderPhysicalMemory(StringBuilder sb) {
            sb.append("physical.memory.total=")
                    .append(numberToUnit(osTotalPhysicalMemorySize.readLong())).append(", ");
            sb.append("physical.memory.free=")
                    .append(numberToUnit(osFreePhysicalMemorySize.readLong())).append(", ");
        }

        private void renderSwap(StringBuilder sb) {
            sb.append("swap.space.total=")
                    .append(numberToUnit(osTotalSwapSpaceSize.readLong())).append(", ");
            sb.append("swap.space.free=")
                    .append(numberToUnit(osFreeSwapSpaceSize.readLong())).append(", ");
        }

        private void renderHeap(StringBuilder sb) {
            sb.append("heap.memory.used=")
                    .append(numberToUnit(runtimeUsedMemory.readLong())).append(", ");
            sb.append("heap.memory.free=")
                    .append(numberToUnit(runtimeFreeMemory.readLong())).append(", ");
            sb.append("heap.memory.total=")
                    .append(numberToUnit(runtimeTotalMemory.readLong())).append(", ");
            sb.append("heap.memory.max=")
                    .append(numberToUnit(runtimeMaxMemory.readLong())).append(", ");
            sb.append("heap.memory.used/total=")
                    .append(percentageString(memoryUsedOfTotalPercentage)).append(", ");
            sb.append("heap.memory.used/max=")
                    .append(percentageString(memoryUsedOfMaxPercentage)).append((", "));
        }

        private void renderEvents(StringBuilder sb) {
            sb.append("event.q.size=")
                    .append(eventQueueSize.readLong()).append(", ");
        }

        private void renderCluster(StringBuilder sb) {
            sb.append("cluster.timeDiff=")
                    .append(clusterTimeDiff.readLong()).append(", ");
        }

        private void renderThread(StringBuilder sb) {
            sb.append("thread.count=")
                    .append(threadThreadCount.readLong()).append(", ");
            sb.append("thread.peakCount=")
                    .append(threadPeakThreadCount.readLong()).append(", ");
        }

        private void renderGc(StringBuilder sb) {
            sb.append("minor.gc.count=")
                    .append(gcMinorCount.readLong()).append(", ");
            sb.append("minor.gc.time=")
                    .append(gcMinorTime.readLong()).append("ms, ");
            sb.append("major.gc.count=")
                    .append(gcMajorCount.readLong()).append(", ");
            sb.append("major.gc.time=")
                    .append(gcMajorTime.readLong()).append("ms, ");

            if (gcUnknownCount.readLong() > 0) {
                sb.append("unknown.gc.count=")
                        .append(gcUnknownCount.readLong()).append(", ");
                sb.append("unknown.gc.time=")
                        .append(gcUnknownTime.readLong()).append("ms, ");
            }
        }

        private void renderNativeMemory(StringBuilder sb) {
            MemoryStats memoryStats = node.getNodeExtension().getMemoryStats();
            if (memoryStats.getMaxNativeMemory() <= 0L) {
                return;
            }

            sb.append("native.memory.used=")
                    .append(numberToUnit(memoryStats.getUsedNativeMemory())).append(", ");
            sb.append("native.memory.free=")
                    .append(numberToUnit(memoryStats.getFreeNativeMemory())).append(", ");
            sb.append("native.memory.total=")
                    .append(numberToUnit(memoryStats.getCommittedNativeMemory())).append(", ");
            sb.append("native.memory.max=")
                    .append(numberToUnit(memoryStats.getMaxNativeMemory())).append(", ");
        }

        private void renderExecutors(StringBuilder sb) {
            sb.append("executor.q.async.size=")
                    .append(executorAsyncQueueSize.readLong()).append(", ");
            sb.append("executor.q.client.size=")
                    .append(executorClientQueueSize.readLong()).append(", ");
            sb.append("executor.q.query.size=")
                    .append(executorQueryQueueSize.readLong()).append(", ");
            sb.append("executor.q.scheduled.size=")
                    .append(executorScheduledQueueSize.readLong()).append(", ");
            sb.append("executor.q.io.size=")
                    .append(executorIoQueueSize.readLong()).append(", ");
            sb.append("executor.q.system.size=")
                    .append(executorSystemQueueSize.readLong()).append(", ");
            sb.append("executor.q.mapLoad.size=")
                    .append(executorMapLoadQueueSize.readLong()).append(", ");
            sb.append("executor.q.mapLoadAllKeys.size=")
                    .append(executorMapLoadAllKeysQueueSize.readLong()).append(", ");
            sb.append("executor.q.cluster.size=")
                    .append(executorClusterQueueSize.readLong()).append(", ");
        }

        private void renderOperationService(StringBuilder sb) {
            sb.append("operations.completed.count=")
                    .append(operationServiceCompletedOperationsCount.readLong()).append(", ");
            sb.append("operations.executor.q.size=")
                    .append(operationServiceExecutorQueueSize.readLong()).append(", ");
            sb.append("operations.executor.priority.q.size=").
                    append(operationServiceExecutorPriorityQueueSize.readLong()).append(", ");
            sb.append("operations.response.q.size=")
                    .append(operationServiceResponseQueueSize.readLong()).append(", ");
            sb.append("operations.running.count=")
                    .append(operationServiceRunningOperationsCount.readLong()).append(", ");
            sb.append("operations.pending.invocations.percentage=")
                    .append(format("%.2f", operationServicePendingInvocationsPercentage.readDouble())).append("%, ");
            sb.append("operations.pending.invocations.count=")
                    .append(operationServicePendingInvocationsCount.readLong()).append(", ");
        }
    }

    /**
     * Given a number, returns that number as a percentage string.
     *
     * @param p the given number
     * @return a string of the given number as a format float with two decimal places and a period
     */
    public static String percentageString(double p) {
        return format("%.2f", p) + "%";
    }

    public static String numberToUnit(long number) {
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
