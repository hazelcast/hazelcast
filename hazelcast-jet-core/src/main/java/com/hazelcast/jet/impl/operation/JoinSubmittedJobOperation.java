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
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;

import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public class JoinSubmittedJobOperation extends AsyncJobOperation {

    public JoinSubmittedJobOperation() {
    }

    public JoinSubmittedJobOperation(long jobId) {
        super(jobId);
    }

    @Override
    protected void doRun() {
        JetService service = getService();
        JobCoordinationService coordinationService = service.getJobCoordinationService();
        CompletableFuture<Void> executionFuture = coordinationService.joinSubmittedJob(jobId());
        executionFuture.whenComplete(withTryCatch(getLogger(), (r, t) -> doSendResponse(peel(t))));
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.JOIN_SUBMITTED_JOB;
    }
}
