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

import com.hazelcast.instance.GroupProperty;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastThreadGroup;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import static java.lang.String.format;

/**
 * The PerformanceMonitor is a tool that provides insights in internal metrics. Currently the content of the
 * {@link MetricsRegistry} is being dumped.
 */
public class PerformanceMonitor {

    final MetricsRegistry metricRegistry;
    final HazelcastInstanceImpl hazelcastInstance;
    final ILogger logger;
    final InternalOperationService operationService;
    final PerformanceLogFile performanceLogFile;
    final boolean humanFriendlyFormat;
    private final Node node;
    private final MonitorThread monitorThread;
    private final boolean enabled;

    public PerformanceMonitor(HazelcastInstanceImpl hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
        this.node = hazelcastInstance.node;
        this.operationService = node.nodeEngine.getOperationService();
        this.logger = node.getLogger(PerformanceMonitor.class);
        this.metricRegistry = hazelcastInstance.node.nodeEngine.getMetricsRegistry();
        this.enabled = node.getGroupProperties().getBoolean(GroupProperty.PERFORMANCE_MONITOR_ENABLED);
        this.humanFriendlyFormat = node.getGroupProperties().getBoolean(GroupProperty.PERFORMANCE_MONITOR_HUMAN_FRIENDLY_FORMAT);
        this.performanceLogFile = new PerformanceLogFile(this);
        this.monitorThread = initMonitorThread();
    }

    private MonitorThread initMonitorThread() {
        if (!enabled) {
            return null;
        }

        HazelcastThreadGroup threadGroup = node.getHazelcastThreadGroup();
        int delaySeconds = node.getGroupProperties().getSeconds(GroupProperty.PERFORMANCE_MONITOR_DELAY_SECONDS);

        return new MonitorThread(threadGroup, delaySeconds);
    }

    public PerformanceMonitor start() {
        if (!enabled) {
            logger.finest("PerformanceMonitor disabled");
            return this;
        }

        logger.info("PerformanceMonitor started");

        if (!node.getGroupProperties().getBoolean(GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED)) {
            logger.info(format("To enable the SlowOperationDetector in the Performance log,"
                    + " set the following property: -D%s=true", GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED));
        }

        monitorThread.start();
        return this;
    }

    private final class MonitorThread extends Thread {
        private static final int DELAY_MILLIS = 1000;
        private final int delaySeconds;

        private MonitorThread(HazelcastThreadGroup threadGroup, int delaySeconds) {
            super(threadGroup.getInternalThreadGroup(), threadGroup.getThreadNamePrefix("PerformanceMonitor"));
            this.delaySeconds = delaySeconds;
        }

        @Override
        public void run() {
            try {
                while (node.getState() == NodeState.ACTIVE) {
                    performanceLogFile.render();
                    sleep();
                }

                // always write the sensors at the end when shutting down.
                performanceLogFile.render();
            } catch (Throwable t) {
                logger.warning(t.getMessage(), t);
            }
        }

        private void sleep() {
            for (int k = 0; k < delaySeconds; k++) {
                try {
                    Thread.sleep(DELAY_MILLIS);
                } catch (InterruptedException e) {
                    // we can eat the interrupt since we'll check node.isActive.
                    return;
                }

                if (performanceLogFile.isRenderingForced()) {
                    logger.info("Detected a request to update the Performance Log");
                    return;
                }
            }
        }
    }
}
