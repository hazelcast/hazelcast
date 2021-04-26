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
import com.hazelcast.jet.impl.execution.init.JetInitDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class SubmitLightJobOperation extends AsyncJobOperation {

    private DAG dag;

    // for deserialization
    public SubmitLightJobOperation() { }

    public SubmitLightJobOperation(long jobId, DAG dag) {
        super(jobId);
        this.dag = dag;
    }

    protected JetService getJetService() {
        assert getServiceName().equals(JetService.SERVICE_NAME) : "Service is not JetService";
        return getService();
    }

    @Override
    protected CompletableFuture<Void> doRun() {
        assert !getNodeEngine().getLocalMember().isLiteMember() : "light job submitted to a lite member";
        return getJetService().getJobCoordinationService().submitLightJob(jobId(), dag);
    }

    @Override
    public int getClassId() {
        return JetInitDataSerializerHook.SUBMIT_LIGHT_JOB_OP;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(dag);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        dag = in.readObject();
    }
}
