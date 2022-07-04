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

package com.hazelcast.internal.management.operation;

import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.ScriptEngineManagerContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import java.security.AccessControlException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Operation to execute script on the node. The script is executed on {@link ExecutionService#MC_EXECUTOR} executor.
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

        Future<Object> future = executionService.submit(
                ExecutionService.MC_EXECUTOR,
                () -> {
                    ManagementCenterConfig managementCenterConfig = getNodeEngine().getConfig().getManagementCenterConfig();
                    if (!managementCenterConfig.isScriptingEnabled()) {
                        throw new AccessControlException("Using ScriptEngine is not allowed on this Hazelcast member.");
                    }
                    ScriptEngineManager scriptEngineManager = ScriptEngineManagerContext.getScriptEngineManager();
                    ScriptEngine scriptEngine = scriptEngineManager.getEngineByName(engine);
                    if (scriptEngine == null) {
                        throw new IllegalArgumentException("Could not find ScriptEngine named '" + engine + "'."
                                + " Please add the corresponding ScriptEngine to the classpath of this Hazelcast member");
                    }
                    scriptEngine.put("hazelcast", getNodeEngine().getHazelcastInstance());
                    try {
                        return scriptEngine.eval(script);
                    } catch (ScriptException e) {
                        // ScriptException's cause is not serializable - we don't need the cause
                        HazelcastException hazelcastException = new HazelcastException(e.getMessage());
                        hazelcastException.setStackTrace(e.getStackTrace());
                        throw hazelcastException;
                    }
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
