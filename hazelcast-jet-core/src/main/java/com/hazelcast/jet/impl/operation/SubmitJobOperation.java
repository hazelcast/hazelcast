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

import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;

public class SubmitJobOperation extends AsyncExecutionOperation implements IdentifiedDataSerializable {

    private Data dag;
    private JobConfig config;
    private volatile CompletableFuture<Boolean> executionFuture;

    public SubmitJobOperation() {
    }

    public SubmitJobOperation(long jobId, Data dag, JobConfig config) {
        super(jobId);
        this.dag = dag;
        this.config = config;
    }

    @Override
    protected void doRun() {
        JetService service = getService();
        executionFuture = service.submitJob(jobId, dag, config);
        executionFuture.whenComplete(withTryCatch(getLogger(), (r, t) -> doSendResponse(peel(t))));
    }

    @Override
    public void cancel() {
        if (executionFuture != null) {
            executionFuture.cancel(true);
        }
    }

    @Override
    public int getId() {
        return JetInitDataSerializerHook.SUBMIT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeData(dag);
        out.writeObject(config);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dag = in.readData();
        config = in.readObject();
    }

}
