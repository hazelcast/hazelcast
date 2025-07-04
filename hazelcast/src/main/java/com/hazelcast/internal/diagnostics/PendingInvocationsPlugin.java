/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.impl.Invocation;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;
import com.hazelcast.internal.util.ItemCounter;

import static com.hazelcast.internal.diagnostics.OperationDescriptors.toOperationDesc;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link DiagnosticsPlugin} that aggregates the pending invocation so that per type of operation, the occurrence
 * count is displayed.
 */
public final class PendingInvocationsPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds this plugin runs.
     * <p>
     * With the pending invocation plugins an aggregation is made per type of operation how many pending
     * invocations there are.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS
            = new HazelcastProperty("hazelcast.diagnostics.pending.invocations.period.seconds", 0, SECONDS);

    /**
     * The minimum number of invocations per type of operation before it start logging this particular operation.
     */
    public static final HazelcastProperty THRESHOLD
            = new HazelcastProperty("hazelcast.diagnostics.pending.invocations.threshold", 1);

    private final InvocationRegistry invocationRegistry;
    private final ItemCounter<String> occurrenceMap = new ItemCounter<>();
    private final HazelcastProperties properties;
    private long periodMillis;
    private int threshold;

    public PendingInvocationsPlugin(ILogger logger, InvocationRegistry invocationRegistry, HazelcastProperties props) {
        super(logger);
        this.invocationRegistry = invocationRegistry;
        properties = props;
        readProperties();
    }

    @Override
    void readProperties() {
        this.periodMillis = properties.getMillis(overrideProperty(PERIOD_SECONDS));
        this.threshold = properties.getInteger(overrideProperty(THRESHOLD));
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        super.onStart();
        logger.info("Plugin:active: period-millis:" + periodMillis + " threshold:" + threshold);
    }

    @Override
    public void onShutdown() {
        clean();
        super.onShutdown();
        logger.info("Plugin:inactive");
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        if (!isActive()) {
            return;
        }
        clean();
        scan();
        render(writer);
    }

    private void clean() {
        occurrenceMap.reset();
    }

    private void scan() {
        for (Invocation invocation : invocationRegistry) {
            occurrenceMap.add(toOperationDesc(invocation.op), 1);
        }
    }

    private void render(DiagnosticsLogWriter writer) {
        writer.startSection("PendingInvocations");
        writer.writeKeyValueEntry("count", invocationRegistry.size());
        renderInvocations(writer);
        writer.endSection();
    }

    private void renderInvocations(DiagnosticsLogWriter writer) {
        writer.startSection("invocations");
        for (String op : occurrenceMap.keySet()) {
            long count = occurrenceMap.get(op);
            if (count < threshold) {
                continue;
            }

            writer.writeKeyValueEntry(op, count);
        }
        writer.endSection();
    }

    // just for testing
    int getThreshold() {
        return threshold;
    }
}
