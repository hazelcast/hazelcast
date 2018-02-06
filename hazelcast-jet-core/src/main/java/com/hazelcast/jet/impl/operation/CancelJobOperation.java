/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

/**
 * Operation sent from user to master member to cancel the job. Master then sends
 * {@link CancelExecutionOperation} to other members.
 */
public class CancelJobOperation extends AbstractJobOperation {

    public CancelJobOperation() {
    }

    public CancelJobOperation(long jobId) {
        super(jobId);
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        JobCoordinationService coordinationService = service.getJobCoordinationService();
        coordinationService.cancelJob(jobId());
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.CANCEL_JOB_OP;
    }

}
