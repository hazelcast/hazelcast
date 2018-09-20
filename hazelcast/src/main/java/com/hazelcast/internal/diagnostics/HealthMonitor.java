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

import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRenderContext;
import com.hazelcast.internal.probing.ProbeRenderer;
import com.hazelcast.internal.probing.ProbeSource;
import com.hazelcast.internal.probing.ProbeUtils;
import com.hazelcast.internal.probing.sources.GcProbeSource;
import com.hazelcast.internal.probing.sources.MachineProbeSource;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.tcp.TcpIpConnectionManager;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.executionservice.impl.ExecutionServiceImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
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

import java.util.HashMap;
import java.util.Map;

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
    private static final long PERCENTAGE_MULTIPLIER = 100L;
    private static final long THRESHOLD_PERCENTAGE_INVOCATIONS = ProbeUtils.toLong(70d);
    private static final long THRESHOLD_INVOCATIONS = 1000;

    final HealthMetrics healthMetrics;

    private final ILogger logger;
    private final Node node;
    private final HealthMonitorLevel monitorLevel;
    private final int thresholdMemoryPercentage;
    private final int thresholdCPUPercentage;
    private final HealthMonitorThread monitorThread;

    public HealthMonitor(Node node) {
        this.node = node;
        this.logger = node.getLogger(HealthMonitor.class);
        this.monitorLevel = getHealthMonitorLevel();
        this.thresholdMemoryPercentage = node.getProperties().getInteger(HEALTH_MONITORING_THRESHOLD_MEMORY_PERCENTAGE);
        this.thresholdCPUPercentage = node.getProperties().getInteger(HEALTH_MONITORING_THRESHOLD_CPU_PERCENTAGE);
        this.monitorThread = initMonitorThread();
        this.healthMetrics = new HealthMetrics(node.nodeEngine.getProbeRegistry());
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
                    switch (monitorLevel) {
                    case NOISY:
                        if (healthMetrics.checkThreshold()) {
                            logDiagnosticsHint();
                        }
                        logger.info(healthMetrics.render());
                        break;
                    case SILENT:
                        if (healthMetrics.checkThreshold()) {
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

    class HealthMetrics implements ProbeRenderer {

        /**
         * Values originally being a double are multiplied by 10k. The original value
         * was in range 0-1. To get this as percent we divided by "just" 100.
         */
        private static final int DOUBLE_TO_PERCENT = 100;

        private final ProbeRegistry registry;
        private final StringBuilder sb = new StringBuilder();
        private final Map<String, Long> metrics = new HashMap<String, Long>();
        private ProbeRenderContext thresholdRenderContext;
        private ProbeRenderContext printoutRenderContext;

        public HealthMetrics(ProbeRegistry registry) {
            this.registry = registry;
        }

        /**
         * The {@link ProbeRenderContext} has to be created after all
         * {@link ProbeSource}s are registered.
         */
        @SuppressWarnings("unchecked")
        private void init() {
            if (thresholdRenderContext == null) {
                thresholdRenderContext = registry.newRenderContext(NodeEngineImpl.class,
                        MachineProbeSource.class);
            }
            if (printoutRenderContext == null) {
                printoutRenderContext = registry.newRenderContext(NodeEngineImpl.class,
                        ClientEngineImpl.class, ClusterServiceImpl.class,
                        ExecutionServiceImpl.class, EventServiceImpl.class,
                        OperationExecutorImpl.class, TcpIpConnectionManager.class,
                        MachineProbeSource.class, GcProbeSource.class);
            }
        }

        @Override
        public void render(CharSequence key, long value) {
            metrics.put(key.toString(), value);
        }

        /**
         * We use a default of zero (not -1) for unknown values as this has been the
         * behavior of the health monitor before.
         */
        private long read(String name) {
            Long val = metrics.get(name);
            return val == null || val.longValue() == -1L ? 0L : val.longValue();
        }

        boolean checkThreshold() {
            // to allow tests to setup data there is this check
            if (metrics.isEmpty()) {
                updateThreshHoldMetrics();
            }
            boolean exceeds = exceedsThreshold();
            metrics.clear();
            return exceeds;
        }

        void updateThreshHoldMetrics() {
            init();
            thresholdRenderContext.render(ProbeLevel.MANDATORY, this);
        }

        boolean exceedsThreshold() {
            double memoryUsedOfMaxPercentage = (PERCENTAGE_MULTIPLIER * read("runtime.usedMemory"))
                    / read("runtime.maxMemory");
            return memoryUsedOfMaxPercentage > thresholdMemoryPercentage
                    || (read("os.processCpuLoad") / DOUBLE_TO_PERCENT) > thresholdCPUPercentage
                    || (read("os.systemCpuLoad") / DOUBLE_TO_PERCENT) > thresholdCPUPercentage
                    || read("operation.invocations.usedPercentage") > THRESHOLD_PERCENTAGE_INVOCATIONS
                    || read("operation.invocations.pending") > THRESHOLD_INVOCATIONS;
        }

        public String render() {
            init();
            printoutRenderContext.render(ProbeLevel.MANDATORY, this);
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
            metrics.clear();
            return sb.toString();
        }

        private void renderConnection() {
            sb.append("connection.active.count=")
            .append(read("tcp.connection.activeCount")).append(", ");
            sb.append("client.connection.count=")
            .append(read("tcp.connection.clientCount")).append(", ");
            sb.append("connection.count=")
            .append(read("tcp.connection.count"));
        }

        private void renderClient() {
            sb.append("clientEndpoint.count=")
            .append(read("client.endpoint.count")).append(", ");
        }

        private void renderProxy() {
            sb.append("proxy.count=")
            .append(read("proxy.proxyCount")).append(", ");
        }

        private void renderLoad() {
            double processCpuLoad = ProbeUtils.doubleValue(read("os.processCpuLoad") * PERCENTAGE_MULTIPLIER);
            sb.append("load.process").append('=')
            .append(format("%.2f", processCpuLoad)).append("%, ");
            double systemCpuLoad = ProbeUtils.doubleValue(read("os.systemCpuLoad") * PERCENTAGE_MULTIPLIER);
            sb.append("load.system").append('=')
            .append(format("%.2f", systemCpuLoad)).append("%, ");

            long systemLoadAverage = read("os.systemLoadAverage");
            if (systemLoadAverage < 0) {
                sb.append("load.systemAverage").append("=n/a ");
            } else {
                sb.append("load.systemAverage").append('=')
                .append(format("%.2f", ProbeUtils.doubleValue(systemLoadAverage))).append(", ");
            }
        }

        private void renderProcessors() {
            sb.append("processors=")
            .append(read("runtime.availableProcessors")).append(", ");
        }

        private void renderPhysicalMemory() {
            sb.append("physical.memory.total=")
            .append(numberToUnit(read("os.totalPhysicalMemorySize"))).append(", ");
            sb.append("physical.memory.free=")
            .append(numberToUnit(read("os.freePhysicalMemorySize"))).append(", ");
        }

        private void renderSwap() {
            sb.append("swap.space.total=")
            .append(numberToUnit(read("os.totalSwapSpaceSize"))).append(", ");
            sb.append("swap.space.free=")
            .append(numberToUnit(read("os.freeSwapSpaceSize"))).append(", ");
        }

        private void renderHeap() {
            sb.append("heap.memory.used=")
            .append(numberToUnit(read("runtime.usedMemory"))).append(", ");
            sb.append("heap.memory.free=")
            .append(numberToUnit(read("runtime.freeMemory"))).append(", ");
            sb.append("heap.memory.total=")
            .append(numberToUnit(read("runtime.totalMemory"))).append(", ");
            sb.append("heap.memory.max=")
            .append(numberToUnit(read("runtime.maxMemory"))).append(", ");
            long usedMemory = read("runtime.usedMemory");
            double memoryUsedOfTotalPercentage = (PERCENTAGE_MULTIPLIER * usedMemory)
                    / (double) read("runtime.totalMemory");
            sb.append("heap.memory.used/total=")
            .append(percentageString(memoryUsedOfTotalPercentage)).append(", ");
            double memoryUsedOfMaxPercentage = (PERCENTAGE_MULTIPLIER * usedMemory)
                    / (double) read("runtime.maxMemory");
            sb.append("heap.memory.used/max=")
            .append(percentageString(memoryUsedOfMaxPercentage)).append((", "));
        }

        private void renderEvents() {
            sb.append("event.q.size=")
            .append(read("event.eventQueueSize")).append(", ");
        }

        private void renderCluster() {
            sb.append("cluster.timeDiff=")
            .append(read("cluster.clock.clusterTimeDiff")).append(", ");
        }

        private void renderThread() {
            sb.append("thread.count=")
            .append(read("thread.threadCount")).append(", ");
            sb.append("thread.peakCount=")
            .append(read("thread.peakThreadCount")).append(", ");
        }

        private void renderGc() {
            sb.append("minor.gc.count=")
            .append(read("gc.minorCount")).append(", ");
            sb.append("minor.gc.time=")
            .append(read("gc.minorTime")).append("ms, ");
            sb.append("major.gc.count=")
            .append(read("gc.majorCount")).append(", ");
            sb.append("major.gc.time=")
            .append(read("gc.majorTime")).append("ms, ");

            long gcUnknownCount = read("gc.unknownCount");
            if (gcUnknownCount > 0) {
                sb.append("unknown.gc.count=")
                .append(gcUnknownCount).append(", ");
                sb.append("unknown.gc.time=")
                .append(read("gc.unknownTime")).append("ms, ");
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
            .append(read("type=internal-executor instance=hz:async queueSize")).append(", ");
            sb.append("executor.q.client.size=")
            .append(read("type=internal-executor instance=hz:client queueSize")).append(", ");
            sb.append("executor.q.query.size=")
            .append(read("type=internal-executor instance=hz:query queueSize")).append(", ");
            sb.append("executor.q.scheduled.size=")
            .append(read("type=internal-executor instance=hz:scheduled queueSize")).append(", ");
            sb.append("executor.q.io.size=")
            .append(read("type=internal-executor instance=hz:io queueSize")).append(", ");
            sb.append("executor.q.system.size=")
            .append(read("type=internal-executor instance=hz:system queueSize")).append(", ");
            sb.append("executor.q.operations.size=")
            .append(read("operation.queueSize")).append(", ");
            sb.append("executor.q.priorityOperation.size=").
            append(read("operation.priorityQueueSize")).append(", ");
            sb.append("operations.completed.count=")
            .append(read("operation.completedCount")).append(", ");

            sb.append("executor.q.mapLoad.size=")
            .append(read("type=internal-executor instance=hz:map-load queueSize")).append(", ");
            sb.append("executor.q.mapLoadAllKeys.size=")
            .append(read("type=internal-executor instance=hz:map-loadAllKeys queueSize")).append(", ");
            sb.append("executor.q.cluster.size=")
            .append(read("type=internal-executor instance=hz:cluster queueSize")).append(", ");
        }

        private void renderOperationService() {
            sb.append("executor.q.response.size=")
            .append(read("operation.responseQueueSize")).append(", ");
            sb.append("operations.running.count=")
            .append(read("operation.runningCount")).append(", ");
            sb.append("operations.pending.invocations.percentage=")
            .append(format("%.2f", ProbeUtils.doubleValue(read("operation.invocations.usedPercentage")))).append("%, ");
            sb.append("operations.pending.invocations.count=")
            .append(read("operation.invocations.pending")).append(", ");
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
