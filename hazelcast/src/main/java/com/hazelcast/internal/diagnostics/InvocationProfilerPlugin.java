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

import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationRegistry;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperty;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A {@link DiagnosticsPlugin} that displays invocation latency information.
 */
public class InvocationProfilerPlugin extends DiagnosticsPlugin {

    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(
            "hazelcast.diagnostics.invocation-profiler.period.seconds", 5, SECONDS);

    private final InvocationRegistry invocationRegistry;
    private final long periodMs;

    public InvocationProfilerPlugin(NodeEngineImpl nodeEngine) {
        super(nodeEngine.getLogger(PendingInvocationsPlugin.class));
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        this.invocationRegistry = operationService.getInvocationRegistry();
        this.periodMs = nodeEngine.getProperties().getMillis(PERIOD_SECONDS);
    }

    @Override
    public long getPeriodMillis() {
        return periodMs;
    }

    @Override
    public void onStart() {
        logger.info("Plugin:active: period-millis:" + periodMs);
    }

    @Override
    public void run(DiagnosticsLogWriter writer) {
        writer.startSection("InvocationProfiler");
        OperationProfilerPlugin.write(writer, invocationRegistry.latencyDistributions());
        writer.endSection();
    }
}

