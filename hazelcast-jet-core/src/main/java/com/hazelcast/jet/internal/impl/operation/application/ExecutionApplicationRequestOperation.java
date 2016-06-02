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

package com.hazelcast.jet.internal.impl.operation.application;

import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.internal.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.ExecuteApplicationRequest;
import com.hazelcast.jet.internal.impl.statemachine.applicationmaster.requests.InterruptApplicationRequest;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class ExecutionApplicationRequestOperation extends AbstractJetApplicationRequestOperation {
    public ExecutionApplicationRequestOperation() {
    }

    public ExecutionApplicationRequestOperation(String name) {
        this(name, null);
    }

    public ExecutionApplicationRequestOperation(String name, NodeEngineImpl nodeEngine) {
        super(name);
        this.setNodeEngine(nodeEngine);
        setServiceName(JetService.SERVICE_NAME);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext applicationContext = resolveApplicationContext();
        ApplicationMaster applicationMaster = applicationContext.getApplicationMaster();

        System.out.println("ExecutionApplicationRequestOperation.run " + applicationContext.getName());

        Future<ApplicationMasterResponse> future = applicationMaster.handleContainerRequest(new ExecuteApplicationRequest());

        JetApplicationConfig config = applicationContext.getJetApplicationConfig();

        long secondsToAwait = config.getJetSecondsToAwait();

        //Waiting for until all containers started
        ApplicationMasterResponse response = future.get(secondsToAwait, TimeUnit.SECONDS);

        if (response != ApplicationMasterResponse.SUCCESS) {
            throw new IllegalStateException("Unable to start containers");
        }

        //Waiting for execution completion
        BlockingQueue<Object> mailBox = applicationMaster.getExecutionMailBox();

        if (mailBox != null) {
            Object result = mailBox.poll(secondsToAwait, TimeUnit.SECONDS);

            if (result == null) {
                applicationMaster.handleContainerRequest(
                        new InterruptApplicationRequest()
                ).
                        get(secondsToAwait, TimeUnit.SECONDS);

                throw new TimeoutException("Timeout while waiting for result. Application has been interrupted");
            }

            System.out.println("ExecutionApplicationRequestOperation.run.finished");

            if (result instanceof Throwable) {
                throw JetUtil.reThrow((Throwable) result);
            }

            if (response != ApplicationMasterResponse.SUCCESS) {
                throw new IllegalStateException("Unable to startProcessors application");
            }
        }
    }
}
