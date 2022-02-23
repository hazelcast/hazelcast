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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.management.ManagementDataSerializerHook;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.executionservice.ExecutionService;

import java.io.IOException;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.peel;
import static com.hazelcast.internal.util.ExceptionUtil.withTryCatch;

/**
 * Operation to change a cluster's state via Management Center.
 */
public class ChangeClusterStateOperation extends AbstractManagementOperation implements AllowedDuringPassiveState {
    private ClusterState newState;

    public ChangeClusterStateOperation() {
    }

    public ChangeClusterStateOperation(ClusterState newState) {
        this.newState = newState;
    }

    @Override
    public void run() {
        ILogger logger = getNodeEngine().getLogger(getClass());
        ExecutionService executionService = getNodeEngine().getExecutionService();
        Future<Void> future = executionService.submit(
                ExecutionService.ASYNC_EXECUTOR,
                () -> {
                    getNodeEngine().getClusterService().changeClusterState(newState);
                    return null;
                });

        executionService.asCompletableFuture(future).whenCompleteAsync(
                withTryCatch(
                        logger,
                        (empty, error) -> sendResponse(error != null ? peel(error) : null)), CALLER_RUNS);
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

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(newState);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        newState = in.readObject();
    }

    @Override
    public int getClassId() {
        return ManagementDataSerializerHook.CHANGE_CLUSTER_STATE;
    }
}
