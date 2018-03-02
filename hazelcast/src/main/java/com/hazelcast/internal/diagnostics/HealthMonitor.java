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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.DoubleGauge;
import com.hazelcast.internal.metrics.LongGauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.spi.properties.GroupProperty;

import static com.hazelcast.internal.diagnostics.HealthMonitorLevel.OFF;
import static com.hazelcast.internal.diagnostics.HealthMonitorLevel.valueOf;
import static com.hazelcast.spi.properties.GroupProperty.HEALTH_MONITORING_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE;
import static com.hazelcast.spi.properties.GroupProperty.HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE;
import static com.hazelcast.util.ThreadUtil.createThreadName;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Health monitor periodically prints logs about related internal metrics using the {@link MetricsRegistry}
 * to provide some clues about the internal Hazelcast state.
 * <p>
 * Health monitor can be configured with system properties.
 * <ul>
 * <li>{@link GroupProperty#HEALTH_MONITORING_LEVEL} This property can be one of the following:
 * <ul>
 * <li>{@link HealthMonitorLevel#NOISY}  => does not check threshold, always prints</li>
 * <li>{@link HealthMonitorLevel#SILENT} => prints only if metrics are above threshold (default)</li>
 * <li>{@link HealthMonitorLevel#OFF}    => does not print anything</li>
 * </ul>
 * </li>
 * <li>{@link GroupProperty#HEALTH_MONITORING_DELAY_SECONDS}
 * Time between printing two logs of health monitor. Default values is 30 seconds.</li>
 * <li>{@link GroupProperty#HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE}
 * Threshold: Percentage of max memory currently in use</li>
 * <li>{@link GroupProperty#HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE}
 * Threshold: CPU system/process load</li>
 * </ul>
 */
public class HealthMonitor {

    private static final String[] UNITS = new String[]{"", "K", "M", "G", "T", "P", "E"};
    private static final double PERCENTAGE_MULTIPLIER = 100d;
    private static final double THRESHOLD_PERCENTAGE_INVOCATIONS = 70;
    private static final double THRESHOLD_INVOCATIONS = 1000;

    final HealthMetrics healthMetrics;

    private final ILogger logger;
    private final Node node;
    private final HealthMonitorLevel monitorLevel;
    private final int thresholdMemoryPercentage;
    private final int thresholdCPUPercentage;
    private final MetricsRegistry metricRegistry;
    private final HealthMonitorThread monitorThread;

    public HealthMonitor(Node node) {
        this.node = node;
        this.logger = node.getLogger(HealthMonitor.class);
        this.metricRegistry = node.nodeEngine.getMetricsRegistry();
        this.monitorLevel = getHealthMonitorLevel();
        this.thresholdMemoryPercentage = node.getProperties().getInteger(HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE);
        this.thresholdCPUPercentage = node.getProperties().getInteger(HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE);
        this.monitorThread = initMonitorThread();
        this.healthMetrics = new HealthMetrics();
    }

    private HealthMonitorThread initMonitorThread() {
        if (monitorLevel == OFF) {
            return null;
        }

        int delaySeconds = node.getProperties().getSeconds(HEALTH_MONITORING_DELAY_SECONDS);
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

    public void stop() {
        if (monitorLevel == OFF) {
            return;
        }

        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            currentThread().interrupt();
        }
        logger.finest("HealthMonitor stopped");
    }

    private HealthMonitorLevel getHealthMonitorLevel() {
        String healthMonitorLevel = node.getProperties().getString(GroupProperty.HEALTH_MONITORING_LEVEL);
        return valueOf(healthMonitorLevel);
    }

    private final class HealthMonitorThread extends Thread {

        private final int delaySeconds;
        private boolean performanceLogHint;

        private HealthMonitorThread(int delaySeconds) {
            super(createThreadName(node.hazelcastInstance.getName(), "HealthMonitor"));
            setDaemon(true);
            this.delaySeconds = delaySeconds;
            this.performanceLogHint = node.getProperties().getBoolean(Diagnostics.ENABLED);
        }

        @Override
        public void run() {
            try {
                while (node.getState() == NodeState.ACTIVE) {
                    healthMetrics.update();

                    switch (monitorLevel) {
                        case NOISY:
                            if (healthMetrics.exceedsThreshold()) {
                                logDiagnosticsHint();
                            }
                            logger.info(healthMetrics.render());
                            break;
                        case SILENT:
                            if (healthMetrics.exceedsThreshold()) {
                                logDiagnosticsHint();
                                logger.info(healthMetrics.render());
                            }
                            break;
                        default:
                            throw new IllegalStateException("Unrecognized HealthMonitorLevel: " + monitorLevel);
                    }

                    try {
                        SECONDS.sleep(delaySeconds);
                    } catch (InterruptedException e) {
                        currentThread().interrupt();
                        return;
                    }
                }
            } catch (OutOfMemoryError e) {
                OutOfMemoryErrorDispatcher.onOutOfMemory(e);
            } catch (Throwable t) {
                logger.warning("Health Monitor failed", t);
            }
        }

        private void logDiagnosticsHint() {
            if (!performanceLogHint) {
                return;
            }

            // we only log the hint once
            performanceLogHint = false;

            logger.info(format("The HealthMonitor has detected a high load on the system. For more detailed information,%n"
                    + "enable the Diagnostics by adding the property -D%s=true", Diagnostics.ENABLED));
        }
    }

    class HealthMetrics {
        final LongGauge clientEndpointCount
                = metricRegistry.newLongGauge("client.endpoint.count");
        final LongGauge clusterTimeDiff
                = metricRegistry.newLongGauge("cluster.clock.clusterTimeDiff");

        final LongGauge executorAsyncQueueSize
                = metricRegistry.newLongGauge("executor.hz:async.queueSize");
        final LongGauge executorClientQueueSize
                = metricRegistry.newLongGauge("executor.hz:client.queueSize");
        final LongGauge executorClusterQueueSize
                = metricRegistry.newLongGauge("executor.hz:cluster.queueSize");
        final LongGauge executorScheduledQueueSize
                = metricRegistry.newLongGauge("executor.hz:scheduled.queueSize");
        final LongGauge executorSystemQueueSize
                = metricRegistry.newLongGauge("executor.hz:system.queueSize");
        final LongGauge executorIoQueueSize
                = metricRegistry.newLongGauge("executor.hz:io.queueSize");
        final LongGauge executorQueryQueueSize
                = metricRegistry.newLongGauge("executor.hz:query.queueSize");
        final LongGauge executorMapLoadQueueSize
                = metricRegistry.newLongGauge("executor.hz:map-load.queueSize");
        final LongGauge executorMapLoadAllKeysQueueSize
                = metricRegistry.newLongGauge("executor.hz:map-loadAllKeys.queueSize");

        final LongGauge eventQueueSize
                = metricRegistry.newLongGauge("event.eventQueueSize");

        final LongGauge gcMinorCount
                = metricRegistry.newLongGauge("gc.minorCount");
        final LongGauge gcMinorTime
                = metricRegistry.newLongGauge("gc.minorTime");
        final LongGauge gcMajorCount
                = metricRegistry.newLongGauge("gc.majorCount");
        final LongGauge gcMajorTime
                = metricRegistry.newLongGauge("gc.majorTime");
        final LongGauge gcUnknownCount
                = metricRegistry.newLongGauge("gc.unknownCount");
        final LongGauge gcUnknownTime
                = metricRegistry.newLongGauge("gc.unknownTime");

        final LongGauge runtimeAvailableProcessors
                = metricRegistry.newLongGauge("runtime.availableProcessors");
        final LongGauge runtimeMaxMemory
                = metricRegistry.newLongGauge("runtime.maxMemory");
        final LongGauge runtimeFreeMemory
                = metricRegistry.newLongGauge("runtime.freeMemory");
        final LongGauge runtimeTotalMemory
                = metricRegistry.newLongGauge("runtime.totalMemory");
        final LongGauge runtimeUsedMemory
                = metricRegistry.newLongGauge("runtime.usedMemory");

        final LongGauge threadPeakThreadCount
                = metricRegistry.newLongGauge("thread.peakThreadCount");
        final LongGauge threadThreadCount
                = metricRegistry.newLongGauge("thread.threadCount");

        final DoubleGauge osProcessCpuLoad
                = metricRegistry.newDoubleGauge("os.processCpuLoad");
        final DoubleGauge osSystemLoadAverage
                = metricRegistry.newDoubleGauge("os.systemLoadAverage");
        final DoubleGauge osSystemCpuLoad
                = metricRegistry.newDoubleGauge("os.systemCpuLoad");
        final LongGauge osTotalPhysicalMemorySize
                = metricRegistry.newLongGauge("os.totalPhysicalMemorySize");
        final LongGauge osFreePhysicalMemorySize
                = metricRegistry.newLongGauge("os.freePhysicalMemorySize");
        final LongGauge osTotalSwapSpaceSize
                = metricRegistry.newLongGauge("os.totalSwapSpaceSize");
        final LongGauge osFreeSwapSpaceSize
                = metricRegistry.newLongGauge("os.freeSwapSpaceSize");

        final LongGauge operationServiceExecutorQueueSize
                = metricRegistry.newLongGauge("operation.queueSize");
        final LongGauge operationServiceExecutorPriorityQueueSize
                = metricRegistry.newLongGauge("operation.priorityQueueSize");
        final LongGauge operationServiceResponseQueueSize
                = metricRegistry.newLongGauge("operation.responseQueueSize");
        final LongGauge operationServiceRunningOperationsCount
                = metricRegistry.newLongGauge("operation.runningCount");
        final LongGauge operationServiceCompletedOperationsCount
                = metricRegistry.newLongGauge("operation.completedCount");
        final LongGauge operationServicePendingInvocationsCount
                = metricRegistry.newLongGauge("operation.invocations.pending");
        final DoubleGauge operationServicePendingInvocationsPercentage
                = metricRegistry.newDoubleGauge("operation.invocations.used");

        final LongGauge proxyCount
                = metricRegistry.newLongGauge("proxy.proxyCount");

        final LongGauge tcpConnectionActiveCount
                = metricRegistry.newLongGauge("tcp.connection.activeCount");
        final LongGauge tcpConnectionCount
                = metricRegistry.newLongGauge("tcp.connection.count");
        final LongGauge tcpConnectionClientCount
                = metricRegistry.newLongGauge("tcp.connection.clientCount");

        private final StringBuilder sb = new StringBuilder();
        private double memoryUsedOfTotalPercentage;
        private double memoryUsedOfMaxPercentage;

        public void update() {
            memoryUsedOfTotalPercentage = (PERCENTAGE_MULTIPLIER * runtimeUsedMemory.read()) / runtimeTotalMemory.read();
            memoryUsedOfMaxPercentage = (PERCENTAGE_MULTIPLIER * runtimeUsedMemory.read()) / runtimeMaxMemory.read();
        }

        boolean exceedsThreshold() {
            if (memoryUsedOfMaxPercentage > thresholdMemoryPercentage) {
                return true;
            }
            if (osProcessCpuLoad.read() > thresholdCPUPercentage) {
                return true;
            }
            if (osSystemCpuLoad.read() > thresholdCPUPercentage) {
                return true;
            }
            if (operationServicePendingInvocationsPercentage.read() > THRESHOLD_PERCENTAGE_INVOCATIONS) {
                return true;
            }
            if (operationServicePendingInvocationsCount.read() > THRESHOLD_INVOCATIONS) {
                return true;
            }
            return false;
        }

        public String render() {
            update();
            sb.setLength(0);
            renderProcessors();
            renderPhysicalMemory();
            renderSwap();
            renderHeap();
            renderNativeMemory();
            renderGc();
            renderLoad();
            renderThread();
            renderCluster();
            renderEvents();
            renderExecutors();
            renderOperationService();
            renderProxy();
            renderClient();
            renderConnection();
            return sb.toString();
        }

        private void renderConnection() {
            sb.append("connection.active.count=")
                    .append(tcpConnectionActiveCount.read()).append(", ");
            sb.append("client.connection.count=")
                    .append(tcpConnectionClientCount.read()).append(", ");
            sb.append("connection.count=")
                    .append(tcpConnectionCount.read());
        }

        private void renderClient() {
            sb.append("clientEndpoint.count=")
                    .append(clientEndpointCount.read()).append(", ");
        }

        private void renderProxy() {
            sb.append("proxy.count=")
                    .append(proxyCount.read()).append(", ");
        }

        private void renderLoad() {
            sb.append("load.process").append('=')
                    .append(format("%.2f", osProcessCpuLoad.read())).append("%, ");
            sb.append("load.system").append('=')
                    .append(format("%.2f", osSystemCpuLoad.read())).append("%, ");

            double value = osSystemLoadAverage.read();
            if (value < 0) {
                sb.append("load.systemAverage").append("=n/a ");
            } else {
                sb.append("load.systemAverage").append('=')
                        .append(format("%.2f", osSystemLoadAverage.read())).append(", ");
            }
        }

        private void renderProcessors() {
            sb.append("processors=")
                    .append(runtimeAvailableProcessors.read()).append(", ");
        }

        private void renderPhysicalMemory() {
            sb.append("physical.memory.total=")
                    .append(numberToUnit(osTotalPhysicalMemorySize.read())).append(", ");
            sb.append("physical.memory.free=")
                    .append(numberToUnit(osFreePhysicalMemorySize.read())).append(", ");
        }

        private void renderSwap() {
            sb.append("swap.space.total=")
                    .append(numberToUnit(osTotalSwapSpaceSize.read())).append(", ");
            sb.append("swap.space.free=")
                    .append(numberToUnit(osFreeSwapSpaceSize.read())).append(", ");
        }

        private void renderHeap() {
            sb.append("heap.memory.used=")
                    .append(numberToUnit(runtimeUsedMemory.read())).append(", ");
            sb.append("heap.memory.free=")
                    .append(numberToUnit(runtimeFreeMemory.read())).append(", ");
            sb.append("heap.memory.total=")
                    .append(numberToUnit(runtimeTotalMemory.read())).append(", ");
            sb.append("heap.memory.max=")
                    .append(numberToUnit(runtimeMaxMemory.read())).append(", ");
            sb.append("heap.memory.used/total=")
                    .append(percentageString(memoryUsedOfTotalPercentage)).append(", ");
            sb.append("heap.memory.used/max=")
                    .append(percentageString(memoryUsedOfMaxPercentage)).append((", "));
        }

        private void renderEvents() {
            sb.append("event.q.size=")
                    .append(eventQueueSize.read()).append(", ");
        }

        private void renderCluster() {
            sb.append("cluster.timeDiff=")
                    .append(clusterTimeDiff.read()).append(", ");
        }

        private void renderThread() {
            sb.append("thread.count=")
                    .append(threadThreadCount.read()).append(", ");
            sb.append("thread.peakCount=")
                    .append(threadPeakThreadCount.read()).append(", ");
        }

        private void renderGc() {
            sb.append("minor.gc.count=")
                    .append(gcMinorCount.read()).append(", ");
            sb.append("minor.gc.time=")
                    .append(gcMinorTime.read()).append("ms, ");
            sb.append("major.gc.count=")
                    .append(gcMajorCount.read()).append(", ");
            sb.append("major.gc.time=")
                    .append(gcMajorTime.read()).append("ms, ");

            if (gcUnknownCount.read() > 0) {
                sb.append("unknown.gc.count=")
                        .append(gcUnknownCount.read()).append(", ");
                sb.append("unknown.gc.time=")
                        .append(gcUnknownTime.read()).append("ms, ");
            }
        }

        private void renderNativeMemory() {
            MemoryStats memoryStats = node.getNodeExtension().getMemoryStats();
            if (memoryStats.getMaxNative() <= 0L) {
                return;
            }

            final long usedNative = memoryStats.getUsedNative();
            sb.append("native.memory.used=")
                    .append(numberToUnit(usedNative)).append(", ");
            sb.append("native.memory.free=")
                    .append(numberToUnit(memoryStats.getFreeNative())).append(", ");
            sb.append("native.memory.total=")
                    .append(numberToUnit(memoryStats.getCommittedNative())).append(", ");
            sb.append("native.memory.max=")
                    .append(numberToUnit(memoryStats.getMaxNative())).append(", ");
            final long maxMeta = memoryStats.getMaxMetadata();
            if (maxMeta > 0) {
                final long usedMeta = memoryStats.getUsedMetadata();
                sb.append("native.meta.memory.used=")
                        .append(numberToUnit(usedMeta)).append(", ");
                sb.append("native.meta.memory.free=")
                        .append(numberToUnit(maxMeta - usedMeta)).append(", ");
                sb.append("native.meta.memory.percentage=")
                        .append(percentageString(PERCENTAGE_MULTIPLIER * usedMeta / (usedNative + usedMeta))).append(", ");
            }
        }

        private void renderExecutors() {
            sb.append("executor.q.async.size=")
                    .append(executorAsyncQueueSize.read()).append(", ");
            sb.append("executor.q.client.size=")
                    .append(executorClientQueueSize.read()).append(", ");
            sb.append("executor.q.query.size=")
                    .append(executorQueryQueueSize.read()).append(", ");
            sb.append("executor.q.scheduled.size=")
                    .append(executorScheduledQueueSize.read()).append(", ");
            sb.append("executor.q.io.size=")
                    .append(executorIoQueueSize.read()).append(", ");
            sb.append("executor.q.system.size=")
                    .append(executorSystemQueueSize.read()).append(", ");
            sb.append("executor.q.operations.size=")
                    .append(operationServiceExecutorQueueSize.read()).append(", ");
            sb.append("executor.q.priorityOperation.size=").
                    append(operationServiceExecutorPriorityQueueSize.read()).append(", ");
            sb.append("operations.completed.count=")
                    .append(operationServiceCompletedOperationsCount.read()).append(", ");
            sb.append("executor.q.mapLoad.size=")
                    .append(executorMapLoadQueueSize.read()).append(", ");
            sb.append("executor.q.mapLoadAllKeys.size=")
                    .append(executorMapLoadAllKeysQueueSize.read()).append(", ");
            sb.append("executor.q.cluster.size=")
                    .append(executorClusterQueueSize.read()).append(", ");
        }

        private void renderOperationService() {
            sb.append("executor.q.response.size=")
                    .append(operationServiceResponseQueueSize.read()).append(", ");
            sb.append("operations.running.count=")
                    .append(operationServiceRunningOperationsCount.read()).append(", ");
            sb.append("operations.pending.invocations.percentage=")
                    .append(format("%.2f", operationServicePendingInvocationsPercentage.read())).append("%, ");
            sb.append("operations.pending.invocations.count=")
                    .append(operationServicePendingInvocationsCount.read()).append(", ");
        }
    }

    /**
     * Given a number, returns that number as a percentage string.
     *
     * @param p the given number
     * @return a string of the given number as a format float with two decimal places and a period
     */
    private static String percentageString(double p) {
        return format("%.2f%%", p);
    }

    @SuppressWarnings("checkstyle:magicnumber")
    private static String numberToUnit(long number) {
        for (int i = 6; i > 0; i--) {
            // 1024 is for 1024 kb is 1 MB etc
            double step = Math.pow(1024, i);
            if (number > step) {
                return format("%3.1f%s", number / step, UNITS[i]);
            }
        }
        return Long.toString(number);
    }
}
