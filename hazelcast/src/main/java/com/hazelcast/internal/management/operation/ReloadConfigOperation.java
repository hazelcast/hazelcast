/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.dynamicconfig.ConfigUpdateResult;
import com.hazelcast.internal.dynamicconfig.ConfigurationService;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import java.util.concurrent.Future;

public class ReloadConfigOperation extends AbstractManagementOperation {
    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.RELOAD_CONFIG_OPERATION;
    }

    @Override
    public void run()
            throws Exception {
        ConfigurationService configService = getService();
        ExecutionService executionService = getNodeEngine().getExecutionService();
        Future<ConfigUpdateResult> future = executionService.submit(
                ExecutionService.MC_EXECUTOR,
                () -> configService.update()
        );
        // returning immediately, the actual response will be submitted back to MC
        // as a ConfigUpdateFinishedEvent or ConfigUpdateFailedEvent
        sendResponse(null);
        executionService.asCompletableFuture(future).whenCompleteAsync(
                (result, throwable) -> {
                    if (throwable != null) {
                        getLogger().severe("dynamic configuration update failed", throwable);
                    }
                }
        );
    }

    @Override
    public String getServiceName() {
        return ConfigurationService.SERVICE_NAME;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }
}
