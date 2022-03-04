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
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Operation sent from master to members to start execution of a job. It is
 * sent after {@link InitExecutionOperation} was successful on all members.
 * The operation doesn't complete immediately, it completes when the execution
 * completes on the member.
 */
public class StartExecutionOperation extends AsyncJobOperation {

    private long executionId;
    private boolean collectMetrics;

    public StartExecutionOperation() {
    }

    public StartExecutionOperation(long jobId, long executionId, boolean collectMetrics) {
        super(jobId);
        this.executionId = executionId;
        this.collectMetrics = collectMetrics;
    }

    @Override
    protected CompletableFuture<RawJobMetrics> doRun() {
        return getJetServiceBackend().getJobExecutionService()
                .beginExecution(getCallerAddress(), jobId(), executionId, collectMetrics);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.START_EXECUTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeBoolean(collectMetrics);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        collectMetrics = in.readBoolean();
    }
}
