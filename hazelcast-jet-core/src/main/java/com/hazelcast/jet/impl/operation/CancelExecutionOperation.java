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
import com.hazelcast.jet.impl.JobExecutionService;
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.ExceptionAction;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.isTopologicalFailure;
import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

/**
 * Operation sent from master to members to cancel their execution.
 * See also {@link CancelJobOperation}.
 */
public class CancelExecutionOperation extends AbstractJobOperation {

    private long executionId;

    public CancelExecutionOperation() {
    }

    public CancelExecutionOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    @Override
    public void run() throws Exception {
        JetService service = getService();
        JobExecutionService executionService = service.getJobExecutionService();
        Address callerAddress = getCallerAddress();
        ExecutionContext ctx = executionService.assertExecutionContext(callerAddress, jobId(), executionId, this);
        ctx.cancelExecution();
    }

    @Override
    public ExceptionAction onInvocationException(Throwable throwable) {
        return isTopologicalFailure(throwable) ? THROW_EXCEPTION : super.onInvocationException(throwable);
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.CANCEL_EXECUTION_OP;
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
