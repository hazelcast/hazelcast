/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.ItemCounter;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import static com.hazelcast.internal.diagnostics.OperationDescriptors.toOperationDesc;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link DiagnosticsPlugin} that displays all invocations that have been
 * executing for some time.
 * <p>
 * It will display the current invocations and the invocation history. For
 * example, if an entry processor has been running for 5 minutes and the
 * {@link #SAMPLE_PERIOD_SECONDS} is set to 1 minute, then there will be
 * 5 samples for that given invocation. This is useful to track which
 * operations have been slow over a longer period of time.
 */
public class InvocationSamplePlugin extends DiagnosticsPlugin {

    /**
     * The sample period in seconds.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty SAMPLE_PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.invocation.sample.period.seconds", 0, SECONDS);

    /**
     * The threshold in seconds to consider an invocation to be slow.
     */
    public static final HazelcastProperty SLOW_THRESHOLD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.invocation.slow.threshold.seconds", 5, SECONDS);

    /**
     * The maximum number of slow invocations to print.
     */
    public static final HazelcastProperty SLOW_MAX_COUNT
            = new HazelcastProperty("hazelcast.diagnostics.invocation.slow.max.count", 100);

    private final InvocationRegistry invocationRegistry;
    private final long samplePeriodMillis;
    private final long thresholdMillis;
    private final int maxCount;
    private final ItemCounter<String> slowOccurrences = new ItemCounter<String>();
    private final ItemCounter<String> occurrences = new ItemCounter<String>();

    public InvocationSamplePlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(PendingInvocationsPlugin.class));
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        this.invocationRegistry = operationService.getInvocationRegistry();
        HazelcastProperties props = nodeEngine.getProperties();
        this.samplePeriodMillis = props.getMillis(SAMPLE_PERIOD_SECONDS);
        this.thresholdMillis = props.getMillis(SLOW_THRESHOLD_SECONDS);
        this.maxCount = props.getInteger(SLOW_MAX_COUNT);
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
    public void run(DiagnosticsLogWriter writer) {
        long now = Clock.currentTimeMillis();

        writer.startSection("Invocations");

        runCurrent(writer, now);

        renderHistory(writer);

        renderSlowHistory(writer);

        writer.endSection();
    }

    private void runCurrent(DiagnosticsLogWriter writer, long now) {
        writer.startSection("Pending");
        int count = 0;
        boolean maxPrinted = false;
        for (Invocation invocation : invocationRegistry) {
            long durationMs = now - invocation.firstInvocationTimeMillis;
            String operationDesc = toOperationDesc(invocation.op);
            occurrences.add(operationDesc, 1);

            if (durationMs < thresholdMillis) {
                // short invocation, lets move on to the next
                continue;
            }

            // it is a slow invocation
            count++;
            if (count < maxCount) {
                writer.writeEntry(invocation.toString() + " duration=" + durationMs + " ms");
            } else if (!maxPrinted) {
                maxPrinted = true;
                writer.writeEntry("max number of invocations to print reached.");
            }
            slowOccurrences.add(operationDesc, 1);
        }
        writer.endSection();
    }

    private void renderHistory(DiagnosticsLogWriter writer) {
        writer.startSection("History");
        for (String item : occurrences.descendingKeys()) {
            writer.writeEntry(item + " samples=" + occurrences.get(item));
        }
        writer.endSection();
    }

    private void renderSlowHistory(DiagnosticsLogWriter writer) {
        writer.startSection("SlowHistory");
        for (String item : slowOccurrences.descendingKeys()) {
            writer.writeEntry(item + " samples=" + slowOccurrences.get(item));
        }
        writer.endSection();
    }
}
