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

package com.hazelcast.internal.management.operation;

import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Operation to execute script on the node. The script is executed on {@link ExecutionService#MC_EXECUTOR} executor.
 *  <p>
 *  Note: This operation reuses {@link ScriptExecutorOperation} to execute the script.
 *  Once MC migrates to client comms, these classes can be merged.
 */
public class RunScriptOperation extends AbstractLocalOperation {

    private final String engine;
    private final String script;

    public RunScriptOperation(String engine, String script) {
        this.engine = engine;
        this.script = script;
    }

    @Override
    public void run() {
        final ILogger logger = getNodeEngine().getLogger(getClass());
        final ExecutionService executionService = getNodeEngine().getExecutionService();
        final ScriptExecutorOperation legacyOperation = new ScriptExecutorOperation(engine, script);
        legacyOperation.setNodeEngine(getNodeEngine());

        Future<Object> future = executionService.submit(
                ExecutionService.MC_EXECUTOR,
                () -> {
                    legacyOperation.run();
                    return legacyOperation.getResponse();
                });

        executionService.asCompletableFuture(future).whenCompleteAsync(
                withTryCatch(
                        logger,
                        (result, error) -> sendResponse(error != null ? peel(error) : result)
                ), CALLER_RUNS
        );
    }

    @Override
    public final Object getResponse() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getServiceName() {
        return ManagementCenterService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
