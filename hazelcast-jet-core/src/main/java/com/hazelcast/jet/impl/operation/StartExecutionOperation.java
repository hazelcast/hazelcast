/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.impl.execution.ExecutionContext;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.jobAndExecutionId;

public class StartExecutionOperation extends AsyncOperation {

    private long executionId;

    public StartExecutionOperation() {
    }

    public StartExecutionOperation(long jobId, long executionId) {
        super(jobId);
        this.executionId = executionId;
    }

    @Override
    protected void doRun() {
        ExecutionContext execCtx = getExecutionCtx();
        Address coordinator = getCallerAddress();
        getLogger().info("Start execution of "
                + jobAndExecutionId(jobId(), executionId) + " from coordinator " + coordinator);
        execCtx.beginExecution().whenComplete(withTryCatch(getLogger(), (i, e) -> {
            if (e instanceof CancellationException) {
                getLogger().fine("Execution of " + jobAndExecutionId(jobId(), executionId)
                        + " was cancelled");
            } else if (e != null) {
                getLogger().fine("Execution of " + jobAndExecutionId(jobId(), executionId)
                        + " completed with failure", e);
            } else {
                getLogger().fine("Execution of " + jobAndExecutionId(jobId(), executionId) + " completed");
            }
            doSendResponse(e);
        }));
    }

    private ExecutionContext getExecutionCtx() {
        JetService service = getService();
        return service.getJobExecutionService().assertExecutionContext(
                getCallerAddress(), jobId(), executionId, this
        );
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.START_EXECUTION_OP;
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
