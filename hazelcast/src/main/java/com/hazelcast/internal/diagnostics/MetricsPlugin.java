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

package com.hazelcast.internal.diagnostics;

import com.hazelcast.internal.metrics.MetricTarget;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.collectors.MetricsCollector;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Set;

import static com.hazelcast.internal.metrics.MetricTarget.DIAGNOSTICS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link DiagnosticsPlugin} that displays the content of the
 * {@link MetricsRegistry}.
 */
public class MetricsPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds the {@link MetricsPlugin} runs.
     * <p>
     * The MetricsPlugin periodically writes the contents of the MetricsRegistry
     * to the logfile. For debugging purposes make sure the minimum metrics
     * level is set to {@link com.hazelcast.internal.metrics.ProbeLevel#DEBUG}
     * with {@link com.hazelcast.config.MetricsConfig#setMinimumLevel(ProbeLevel)}.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.metrics.period.seconds", 60, SECONDS);

    private final MetricsRegistry metricsRegistry;
    private final long periodMillis;
    private final MetricsCollectorImpl probeRenderer = new MetricsCollectorImpl();

    public MetricsPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(MetricsPlugin.class), nodeEngine.getMetricsRegistry(), nodeEngine.getProperties());
    }

    public MetricsPlugin(ILogger logger, MetricsRegistry metricsRegistry, HazelcastProperties properties) {
        super(logger);
        this.metricsRegistry = metricsRegistry;
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
    public void run(DiagnosticsLogWriter writer) {
        probeRenderer.writer = writer;
        // we set the time explicitly so that for this particular rendering of the probes, all metrics have exactly
        // the same timestamp
        probeRenderer.timeMillis = System.currentTimeMillis();
        metricsRegistry.collect(probeRenderer);
        probeRenderer.writer = null;
    }

    private static class MetricsCollectorImpl implements MetricsCollector {
        private static final String SECTION_NAME = "Metric";

        private DiagnosticsLogWriter writer;
        private long timeMillis;

        @Override
        public void collectLong(String name, long value, Set<MetricTarget> excludedMetricTargets) {
            if (!excludedMetricTargets.contains(DIAGNOSTICS)) {
                writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name, value);
            }
        }

        @Override
        public void collectDouble(String name, double value, Set<MetricTarget> excludedMetricTargets) {
            if (!excludedMetricTargets.contains(DIAGNOSTICS)) {
                writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name, value);
            }
        }

        @Override
        public void collectException(String name, Exception e, Set<MetricTarget> excludedMetricTargets) {
            if (!excludedMetricTargets.contains(DIAGNOSTICS)) {
                writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name, e.getClass().getName() + ':' + e.getMessage());
            }
        }

        @Override
        public void collectNoValue(String name, Set<MetricTarget> excludedMetricTargets) {
            if (!excludedMetricTargets.contains(DIAGNOSTICS)) {
                writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name, "NA");
            }
        }
    }
}
