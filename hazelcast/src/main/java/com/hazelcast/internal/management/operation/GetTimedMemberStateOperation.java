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
import com.hazelcast.internal.management.TimedMemberState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.AbstractLocalOperation;

import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Operation to fetch JSON representation of {@link TimedMemberState} from the node.
 * The operation is executed on {@link ExecutionService#MC_EXECUTOR} executor.
 */
public class GetTimedMemberStateOperation extends AbstractLocalOperation {

    @Override
    public void run() {
        final ILogger logger = getNodeEngine().getLogger(getClass());
        final ManagementCenterService mcs = ((NodeEngineImpl) getNodeEngine()).getManagementCenterService();
        final ExecutionService executionService = getNodeEngine().getExecutionService();
        Future<String> future = executionService.submit(
                ExecutionService.MC_EXECUTOR,
                () -> mcs != null ? mcs.getTimedMemberStateJson().orElse(null) : null
        );

        executionService.asCompletableFuture(future).whenComplete(
                withTryCatch(
                        logger,
                        (result, error) -> sendResponse(error != null ? peel(error) : result)
                )
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
