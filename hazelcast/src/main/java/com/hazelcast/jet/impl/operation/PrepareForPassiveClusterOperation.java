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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.spi.impl.operationservice.UrgentSystemOperation;

import java.util.concurrent.CompletableFuture;

/**
 * Sent from the member that initiates a cluster state change to the master.
 * When the operation completes, all jobs have been terminated.
 */
public class PrepareForPassiveClusterOperation extends AsyncOperation implements UrgentSystemOperation {

    public PrepareForPassiveClusterOperation() {
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return getJobCoordinationService().prepareForPassiveClusterState();
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.PREPARE_FOR_PASSIVE_CLUSTER_OP;
    }
}
