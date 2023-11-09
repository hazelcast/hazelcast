/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JobCoordinationService;
import com.hazelcast.jet.impl.MasterContext;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.config.JobConfigArguments.KEY_JOB_IS_NOT_SUSPENDABLE;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_FORCEFUL;
import static com.hazelcast.jet.impl.TerminationMode.RESTART_GRACEFUL;
import static com.hazelcast.jet.impl.TerminationMode.SUSPEND_FORCEFUL;
import static com.hazelcast.jet.impl.TerminationMode.SUSPEND_GRACEFUL;
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
            getJobCoordinationService().terminateLightJob(jobId(), true);
            return completedFuture(null);
        } else {
            JobCoordinationService jobCoordinationService = getJobCoordinationService();
            return checkJobIsNotSuspendable(jobCoordinationService)
                    ? jobCoordinationService.terminateJob(jobId(), terminationMode, true)
                    : jobCoordinationService.terminateJob(jobId(), TerminationMode.CANCEL_FORCEFUL, false);
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

    private boolean checkJobIsNotSuspendable(JobCoordinationService jcs) {
        MasterContext masterContext = jcs.getMasterContext(jobId());
        JobConfig jobConfig = masterContext.jobConfig();

        if ((terminationMode == SUSPEND_GRACEFUL || terminationMode == SUSPEND_FORCEFUL
                || terminationMode == RESTART_GRACEFUL || terminationMode == RESTART_FORCEFUL)) {
            Boolean argument = jobConfig.getArgument(KEY_JOB_IS_NOT_SUSPENDABLE);
            return argument != null && !argument;
        }
        return true;
    }
}
