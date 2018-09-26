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

import com.hazelcast.internal.metrics.MetricsCollector;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.internal.diagnostics.Diagnostics.METRICS_LEVEL;
import static com.hazelcast.internal.diagnostics.Diagnostics.PREFIX;
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
     * to the logfile. For debugging purposes make sure the
     * {@link Diagnostics#METRICS_LEVEL} is set to debug.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty(PREFIX + ".metrics.period.seconds", 60, SECONDS);

    private final MetricsRegistry metricsRegistry;
    private final long periodMillis;
    private final MetricsRenderer probeRenderer = new MetricsRenderer();
    private final ProbeLevel probeLevel;

    public MetricsPlugin(NodeEngineImpl nodeEngine) {
        this(nodeEngine.getLogger(MetricsPlugin.class), nodeEngine.getMetricsRegistry(), nodeEngine.getProperties());
    }

    public MetricsPlugin(ILogger logger, MetricsRegistry metricsRegistry, HazelcastProperties properties) {
        super(logger);
        this.probeLevel = properties.getEnum(METRICS_LEVEL, ProbeLevel.class);
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
        metricsRegistry.collect(probeRenderer, probeLevel);
        probeRenderer.writer = null;
    }

    private static class MetricsRenderer implements MetricsCollector {
        private static final String SECTION_NAME = "Metric";

        private DiagnosticsLogWriter writer;
        private long timeMillis;

        @Override
        public void collectLong(CharSequence name, long value) {
            //todo: name litter
            writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name.toString(), value);
        }

        @Override
        public void collectDouble(CharSequence name, double value) {
            //todo: name litter
            writer.writeSectionKeyValue(SECTION_NAME, timeMillis, name.toString(), value);
        }
    }
}
