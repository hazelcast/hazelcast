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

package com.hazelcast.jet.impl.operation;

import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;

import java.util.concurrent.CompletableFuture;

public class CancelLightJobOperation extends AsyncJobOperation {

    // for deserialization
    public CancelLightJobOperation() { }

    public CancelLightJobOperation(long jobId) {
        super(jobId);
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not JetService";
        return getService();
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        return getJobCoordinationService().terminateLightJob(jobId());
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.CANCEL_LIGHT_JOB_OP;
    }
}
