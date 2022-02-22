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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.security.AccessControlException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

/**
 *  Operation to run console command. The command is executed on {@link ExecutionService#MC_EXECUTOR} executor.
 */
public class RunConsoleCommandOperation extends AbstractLocalOperation {

    private final String command;
    private final String namespace;

    public RunConsoleCommandOperation(String command, String namespace) {
        this.command = command;
        this.namespace = namespace;
    }

    @Override
    public void run() throws Exception {
        final ManagementCenterService mcs = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        if (mcs == null) {
            sendResponse(new HazelcastException("ManagementCenterService is not initialized yet"));
            return;
        }
        final ILogger logger = getNodeEngine().getLogger(getClass());
        final ExecutionService executionService = getNodeEngine().getExecutionService();

        Future<String> future = executionService.submit(
                ExecutionService.MC_EXECUTOR,
                () -> {
                    ManagementCenterConfig mcConfig = getNodeEngine().getConfig().getManagementCenterConfig();
                    if (!mcConfig.isConsoleEnabled()) {
                        throw new AccessControlException("Using Console is not allowed on this Hazelcast member.");
                    }
                    try {
                        final String ns = namespace;
                        String cmd = command;
                        if (!isNullOrEmpty(ns)) {
                            // set namespace as a part of the command
                            cmd = ns + "__" + cmd;
                        }
                        return mcs.runConsoleCommand(cmd);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw e;
                    }
                });

        executionService.asCompletableFuture(future).whenCompleteAsync(
                withTryCatch(
                        logger,
                        (output, error) -> sendResponse(error != null ? peel(error) : output)
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
