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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ItemCounter;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link PerformanceMonitorPlugin} that displays all invocations that have been executing for some time.
 *
 * It will display the current invocations.
 *
 * But it will also display the history. E.g. if a entry processor has been running for 5 minutes and
 * the {@link #SAMPLE_PERIOD_SECONDS is set to 1 minute, then there will be 5 samples for that given invocation.
 * This is useful to track which operations have been slow over a longer period of time.
 */
public class InvocationPlugin extends PerformanceMonitorPlugin {

    /**
     * The sample period in seconds for the {@link InvocationPlugin}.
     *
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty SAMPLE_PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.performance.monitor.invocation.sample.period.seconds", 0, SECONDS);

    /**
     * The threshold in seconds for the {@link InvocationPlugin} to consider an invocation to be slow.
     */
    public static final HazelcastProperty SLOW_THRESHOLD_SECONDS
            = new HazelcastProperty("hazelcast.performance.monitor.invocation.slow.threshold.seconds", 5, SECONDS);

    private final InvocationRegistry invocationRegistry;
    private final long samplePeriodMillis;
    private final long thresholdMillis;
    private final ILogger logger;
    private final ItemCounter<String> slowOccurrences = new ItemCounter<String>();
    private final ItemCounter<String> occurrences = new ItemCounter<String>();

    public InvocationPlugin(NodeEngineImpl nodeEngine) {
        InternalOperationService operationService = nodeEngine.getOperationService();
        this.invocationRegistry = ((OperationServiceImpl) operationService).getInvocationRegistry();
        this.logger = nodeEngine.getLogger(PendingInvocationsPlugin.class);
        HazelcastProperties props = nodeEngine.getProperties();
        this.samplePeriodMillis = props.getMillis(SAMPLE_PERIOD_SECONDS);
        this.thresholdMillis = props.getMillis(SLOW_THRESHOLD_SECONDS);
    }

    @Override
    public long getPeriodMillis() {
        return samplePeriodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + samplePeriodMillis + " threshold-millis:" + thresholdMillis);
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        long now = Clock.currentTimeMillis();

        writer.startSection("Invocations");

        runCurrent(writer, now);

        renderHistory(writer);

        renderSlowHistory(writer);

        writer.endSection();
    }

    private void runCurrent(PerformanceLogWriter writer, long now) {
        writer.startSection("Pending");
        for (Invocation invocation : invocationRegistry) {
            long durationMs = now - invocation.firstInvocationTimeMillis;
            if (durationMs >= thresholdMillis) {
                writer.writeEntry(invocation.toString() + " duration=" + durationMs + " ms");
                slowOccurrences.add(invocation.op.getClass().getName(), 1);
            }

            occurrences.add(invocation.op.getClass().getName(), 1);
        }
        writer.endSection();
    }

    private void renderHistory(PerformanceLogWriter writer) {
        writer.startSection("History");
        for (String item : occurrences.descendingKeys()) {
            writer.writeEntry(item + " samples=" + occurrences.get(item));
        }
        writer.endSection();
    }

    private void renderSlowHistory(PerformanceLogWriter writer) {
        writer.startSection("SlowHistory");
        for (String item : slowOccurrences.descendingKeys()) {
            writer.writeEntry(item + " samples=" + slowOccurrences.get(item));
        }
        writer.endSection();
    }
}
