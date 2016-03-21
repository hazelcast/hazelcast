/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link PerformanceMonitorPlugin} that displays the content of the {@link MetricsRegistry}.
 */
public class MetricsPlugin extends PerformanceMonitorPlugin {

    /**
     * The period in seconds the PerformanceMonitor MetricsPlugin runs.
     *
     * The MetricsPlugin does nothing more than frequently writing the content of the MetricsRegistry
     * to the logfile. For debugging purposes make sure the
     * {@link GroupProperty#PERFORMANCE_METRICS_LEVEL} is set to debug.
     *
     * This plugin is very cheap to use.
     *
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.performance.monitor.metrics.period.seconds", 60, SECONDS);

    private final MetricsRegistry metricsRegistry;
    private final long periodMillis;
    private final ILogger logger;
    private final ProbeRendererImpl probeRenderer = new ProbeRendererImpl();

    public MetricsPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(MetricsPlugin.class), nodeEngine.getMetricsRegistry(), nodeEngine.getProperties());
    }

    public MetricsPlugin(ILogger logger, MetricsRegistry metricsRegistry, HazelcastProperties properties) {
        this.metricsRegistry = metricsRegistry;
        this.logger = logger;
        this.periodMillis = properties.getMillis(PERIOD_SECONDS);
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        writer.startSection("Metrics");
        probeRenderer.writer = writer;
        metricsRegistry.render(probeRenderer);
        probeRenderer.writer = null;
        writer.endSection();
    }

    private static class ProbeRendererImpl implements ProbeRenderer {

        private PerformanceLogWriter writer;

        @Override
        public void renderLong(String name, long value) {
            writer.writeKeyValueEntry(name, value);
        }

        @Override
        public void renderDouble(String name, double value) {
            writer.writeKeyValueEntry(name, value);
        }

        @Override
        public void renderException(String name, Exception e) {
            writer.writeKeyValueEntry(name, e.getClass().getName() + ':' + e.getMessage());
        }

        @Override
        public void renderNoValue(String name) {
            writer.writeKeyValueEntry(name, "NA");
        }
    }
}
