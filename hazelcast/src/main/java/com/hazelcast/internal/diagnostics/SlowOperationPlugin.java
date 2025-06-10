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


import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.List;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link DiagnosticsPlugin} that displays the slow executing operations.
 * <p>
 * For more information see {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
 */
public class SlowOperationPlugin extends DiagnosticsPlugin {

    /**
     * The period in seconds the SlowOperationPlugin runs.
     * <p>
     * With the slow operation plugin, slow executing operation can be found. This is done by checking
     * on the caller side which operations take a lot of time executing.
     * <p>
     * This plugin is very cheap to use.
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(
            "hazelcast.diagnostics.slowoperations.period.seconds", 60, SECONDS);

    private final OperationServiceImpl operationService;
    private final HazelcastProperties properties;
    private long periodMillis;

    public SlowOperationPlugin(ILogger logger, OperationServiceImpl operationService, HazelcastProperties props) {
        super(logger);
        this.operationService = operationService;
        this.properties = props;
        readProperties();
    }

    private long readPeriodMillis() {
        if (!properties.getBoolean(ClusterProperty.SLOW_OPERATION_DETECTOR_ENABLED)) {
            return NOT_SCHEDULED_PERIOD_MS;
        }

        return properties.getMillis(overrideProperty(PERIOD_SECONDS));
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        super.onStart();
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public void onShutdown() {
        super.onShutdown();
        logger.info("Plugin:inactive");
    }

    @Override
    void readProperties() {
        this.periodMillis = readPeriodMillis();
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        if (!isActive()) {
            return;
        }
        List<SlowOperationDTO> slowOperations = operationService.getSlowOperationDTOs();
        writer.startSection("SlowOperations");

        if (!slowOperations.isEmpty()) {
            for (SlowOperationDTO slowOperation : slowOperations) {
                render(writer, slowOperation);
            }
        }

        writer.endSection();
    }

    private void render(DiagnosticsLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection(slowOperation.operation);
        writer.writeKeyValueEntry("invocations", slowOperation.totalInvocations);

        renderStackTrace(writer, slowOperation);
        renderInvocations(writer, slowOperation);

        writer.endSection();
    }

    private void renderInvocations(DiagnosticsLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection("slowInvocations");
        for (SlowOperationInvocationDTO invocation : slowOperation.invocations) {
            writer.writeKeyValueEntry("startedAt", invocation.startedAt);
            writer.writeKeyValueEntryAsDateTime("started(date-time)", invocation.startedAt);
            writer.writeKeyValueEntry("duration(ms)", invocation.durationMs);
            writer.writeKeyValueEntry("operationDetails", invocation.operationDetails);
        }
        writer.endSection();
    }

    private void renderStackTrace(DiagnosticsLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection("stackTrace");
        // this is quite inefficient due to object allocations; it would be cheaper to manually traverse
        String[] stackTraceLines = slowOperation.stackTrace.split(System.lineSeparator());
        for (String stackTraceLine : stackTraceLines) {
            writer.writeEntry(stackTraceLine);
        }
        writer.endSection();
    }
}
