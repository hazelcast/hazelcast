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

package com.hazelcast.jet.impl.operation.application;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanBuilderRequest;
import com.hazelcast.jet.impl.statemachine.applicationmaster.requests.ExecutionPlanReadyRequest;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;


public class SubmitApplicationRequestOperation extends AbstractJetApplicationRequestOperation {
    private DAG dag;

    public SubmitApplicationRequestOperation() {
        super();
    }

    public SubmitApplicationRequestOperation(String name, DAG dag) {
        this(name, dag, null);
    }

    public SubmitApplicationRequestOperation(String name, DAG dag, NodeEngineImpl nodeEngine) {
        super(name);
        this.dag = dag;
        setNodeEngine(nodeEngine);
        setServiceName(JetService.SERVICE_NAME);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        this.dag.validate();

        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();

        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(
                new ExecutionPlanBuilderRequest(this.dag)
        );

        JetApplicationConfig config = applicationContext.getJetApplicationConfig();
        long secondsToAwait = config.getJetSecondsToAwait();

        try {
            ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable to submit dag");
            }

            response =
                    applicationMaster.handleContainerRequest(new ExecutionPlanReadyRequest()).
                            get(secondsToAwait, TimeUnit.SECONDS);

            applicationMaster.setDag(this.dag);

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable to submit dag");
            }
        } catch (Throwable e) {
            getLogger().warning(e.getMessage(), e);
            throw JetUtil.reThrow(e);
        }
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
