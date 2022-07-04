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

import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Operation sent from client to coordinator member to terminate particular
 * job. See also {@link TerminateExecutionOperation}, which is sent from
 * coordinator to members to terminate execution.
 */
public class TerminateJobOperation extends AsyncJobOperation {

    private TerminationMode terminationMode;
    private boolean isLightJob;

    public TerminateJobOperation() {
    }

    public TerminateJobOperation(long jobId, TerminationMode mode, boolean isLightJob) {
        super(jobId);
        this.terminationMode = mode;
        this.isLightJob = isLightJob;
        assert !isLightJob || terminationMode == TerminationMode.CANCEL_FORCEFUL;
    }

    @Override
    public CompletableFuture<Void> doRun() {
        if (isLightJob) {
            getJobCoordinationService().terminateLightJob(jobId());
            return completedFuture(null);
        } else {
            return getJobCoordinationService().terminateJob(jobId(), terminationMode);
        }
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.TERMINATE_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeByte(terminationMode.ordinal());
        out.writeBoolean(isLightJob);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        terminationMode = TerminationMode.values()[in.readByte()];
        isLightJob = in.readBoolean();
    }
}
