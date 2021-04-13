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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.JetService;
import com.hazelcast.jet.impl.Timers;
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SubmitLightJobOperation extends AsyncOperation {

    private long jobId;
    private DAG dag;

    // for deserialization
    public SubmitLightJobOperation() { }

    public SubmitLightJobOperation(long jobId, DAG dag) {
        this.jobId = jobId;
        this.dag = dag;
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not JetService";
        return getService();
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        Timers.i().submitLightJobOperation_run.start();
        long jobId = getJetService().getJobRepository().newJobId();
        assert !getNodeEngine().getLocalMember().isLiteMember() : "light job submitted to a lite member";
        return new LightMasterContext(getNodeEngine(), dag, jobId)
                .start()
                .whenComplete((t, r) -> Timers.i().submitLightJobOperation_run.stop());
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SUBMIT_LIGHT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(jobId);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        jobId = in.readLong();
        dag = in.readObject();
    }
}
