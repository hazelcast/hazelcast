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
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.TerminationMode;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;

import javax.annotation.Nullable;
import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isRestartableException;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

/**
 * Operation sent from master to members to terminate execution of particular
 * job. See also {@link TerminateJobOperation}, which is sent from client to
 * coordinator to initiate the termination.
 */
public class TerminateExecutionOperation extends AbstractJobOperation {

    private long executionId;
    private TerminationMode mode;

    public TerminateExecutionOperation() {
    }

    public TerminateExecutionOperation(long jobId, long executionId, @Nullable TerminationMode mode) {
        super(jobId);
        this.executionId = executionId;
        this.mode = mode;
    }

    @Override
    public void run() {
        JetService service = getService();
        JobExecutionService executionService = service.getJobExecutionService();
        Address callerAddress = getCallerAddress();
        ExecutionContext ctx = executionService.assertExecutionContext(callerAddress, jobId(), executionId,
                getClass().getSimpleName());
        ctx.terminateExecution(mode);
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isRestartableException(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.TERMINATE_EXECUTION_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(executionId);
        out.writeByte(mode != null ? mode.ordinal() : -1);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        executionId = in.readLong();
        byte modeOrdinal = in.readByte();
        mode = modeOrdinal < 0 ? null : TerminationMode.values()[modeOrdinal];
    }
}
