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

import java.io.IOException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.jet.api.hazelcast.JetService;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachineRequest;


public class ApplicationEventOperation extends AbstractJetApplicationRequestOperation {
    private ApplicationEvent applicationEvent;

    public ApplicationEventOperation() {
    }

    public ApplicationEventOperation(ApplicationEvent applicationEvent,
                                     String name) {
        this(applicationEvent, name, null);
    }

    public ApplicationEventOperation(ApplicationEvent applicationEvent,
                                     String name,
                                     NodeEngineImpl nodeEngine) {
        super(name);
        this.applicationEvent = applicationEvent;
        setNodeEngine(nodeEngine);
        setServiceName(JetService.SERVICE_NAME);
    }

    @Override
    public void run() throws Exception {
        ApplicationContext context = resolveApplicationContext();

        synchronized (context) {
            ApplicationStateMachine applicationStateMachine =
                    context.getApplicationStateMachine();

            JetApplicationConfig config = context.getJetApplicationConfig();
            long secondsToAwait = config.getJetSecondsToAwait();

            Future future = applicationStateMachine.handleRequest(
                    new ApplicationStateMachineRequest(this.applicationEvent)
            );

            context.getExecutorContext().getApplicationStateMachineExecutor().wakeUp();
            future.get(secondsToAwait, TimeUnit.SECONDS);
        }
    }

    @Override
    public void writeInternal(ObjectDataOutput out)
            throws IOException {
        super.writeInternal(out);
        out.writeObject(this.applicationEvent);
    }

    @Override
    public void readInternal(ObjectDataInput in)
            throws IOException {
        super.readInternal(in);
        this.applicationEvent = in.readObject();
    }
}
