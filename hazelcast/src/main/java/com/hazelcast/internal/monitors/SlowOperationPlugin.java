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

import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.internal.management.dto.SlowOperationInvocationDTO;
import com.hazelcast.internal.properties.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;

import java.util.List;

import static com.hazelcast.internal.properties.GroupProperty.PERFORMANCE_MONITOR_SLOW_OPERATIONS_PERIOD_SECONDS;
import static com.hazelcast.internal.properties.GroupProperty.SLOW_OPERATION_DETECTOR_ENABLED;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;

/**
 * A {@link PerformanceMonitorPlugin} that displays the slow executing operations. For more information see
 * {@link com.hazelcast.spi.impl.operationexecutor.slowoperationdetector.SlowOperationDetector}.
 */
public class SlowOperationPlugin extends PerformanceMonitorPlugin {

    private final InternalOperationService operationService;
    private final long periodMillis;
    private final ILogger logger;

    public SlowOperationPlugin(NodeEngineImpl nodeEngine) {
        this.logger = nodeEngine.getLogger(SlowOperationPlugin.class);
        this.operationService = nodeEngine.getOperationService();
        this.periodMillis = getPeriodMillis(nodeEngine);
    }

    private long getPeriodMillis(NodeEngineImpl nodeEngine) {
        GroupProperties props = nodeEngine.getGroupProperties();
        if (!props.getBoolean(SLOW_OPERATION_DETECTOR_ENABLED)) {
            return DISABLED;
        }

        return props.getMillis(PERFORMANCE_MONITOR_SLOW_OPERATIONS_PERIOD_SECONDS);
    }

    @Override
    public long getPeriodMillis() {
        return periodMillis;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active, period-millis:" + periodMillis);
    }

    @Override
    public void run(PerformanceLogWriter writer) {
        List<SlowOperationDTO> slowOperations = operationService.getSlowOperationDTOs();
        writer.startSection("SlowOperations");

        if (slowOperations.size() > 0) {
            for (SlowOperationDTO slowOperation : slowOperations) {
                render(writer, slowOperation);
            }
        }

        writer.endSection();
    }

    private void render(PerformanceLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection(slowOperation.operation);
        writer.writeKeyValueEntry("invocations", slowOperation.totalInvocations);

        renderStackTrace(writer, slowOperation);
        renderInvocations(writer, slowOperation);

        writer.endSection();
    }

    private void renderInvocations(PerformanceLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection("slowInvocations");
        for (SlowOperationInvocationDTO invocation : slowOperation.invocations) {
            writer.writeKeyValueEntry("startedAt", invocation.startedAt);
            writer.writeKeyValueEntry("durationNs", invocation.durationMs);
            writer.writeKeyValueEntry("operationDetails", invocation.operationDetails);
        }
        writer.endSection();
    }

    private void renderStackTrace(PerformanceLogWriter writer, SlowOperationDTO slowOperation) {
        writer.startSection("stackTrace");
        // this is quite inefficient due to object allocations; it would be cheaper to manually traverse
        String[] stackTraceLines = slowOperation.stackTrace.split(LINE_SEPARATOR);
        for (String stackTraceLine : stackTraceLines) {
            writer.writeEntry(stackTraceLine);
        }
        writer.endSection();
    }
}
