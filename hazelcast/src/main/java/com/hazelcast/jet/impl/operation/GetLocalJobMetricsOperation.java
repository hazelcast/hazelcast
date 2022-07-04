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

import com.hazelcast.jet.impl.JetServiceBackend;
import com.hazelcast.jet.impl.exception.ExecutionNotFoundException;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * An operation sent from the master to all members to query metrics for a
 * specific execution ID.
 */
public class GetLocalJobMetricsOperation extends AbstractJobOperation {

    private long executionId;
    private Object response;

    public GetLocalJobMetricsOperation() {
    }

    public GetLocalJobMetricsOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    @Override
    public void run() {
        JetServiceBackend service = getJetServiceBackend();
        ExecutionContext executionContext = service.getJobExecutionService().getExecutionContext(executionId);
        if (executionContext == null) {
            throw new ExecutionNotFoundException(executionId);
        }
        response = executionContext.getJobMetrics();
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.GET_LOCAL_JOB_METRICS_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
    }
}
