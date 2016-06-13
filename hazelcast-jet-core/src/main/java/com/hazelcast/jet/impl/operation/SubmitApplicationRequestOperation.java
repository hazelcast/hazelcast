/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.impl.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanBuilderRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanReadyRequest;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;


public class SubmitApplicationRequestOperation extends AsyncJetOperation {
    private DAG dag;

    @SuppressWarnings("unused")
    public SubmitApplicationRequestOperation() {
    }

    public SubmitApplicationRequestOperation(String name, DAG dag) {
        super(name);
        this.dag = dag;
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = getApplicationContext();
        this.dag.validate();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();

        ICompletableFuture<ApplicationMasterResponse> builderFuture =
                applicationMaster.handleContainerRequest(new ExecutionPlanBuilderRequest(this.dag));

        builderFuture.andThen(new ContainerRequestCallback(this, "Unable to submit DAG", () -> {
            ICompletableFuture<ApplicationMasterResponse> readyFuture
                    = applicationMaster.handleContainerRequest(new ExecutionPlanReadyRequest());

            readyFuture.andThen(new ContainerRequestCallback(SubmitApplicationRequestOperation.this,
                    "Unable to submit DAG", () -> {
                applicationMaster.setDag(dag);
                sendResponse(true);
            }));
        }));
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(dag);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        dag = in.readObject();
    }
}
