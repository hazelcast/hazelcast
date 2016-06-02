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

package com.hazelcast.jet.internal.impl.statemachine.applicationmaster.processors;

import com.hazelcast.jet.internal.api.Dummy;
import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.application.ExecutorContext;
import com.hazelcast.jet.internal.api.container.ContainerPayLoadProcessor;
import com.hazelcast.jet.internal.api.container.ProcessingContainer;
import com.hazelcast.jet.internal.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.internal.api.executor.ApplicationTaskContext;
import com.hazelcast.jet.internal.impl.statemachine.container.requests.ContainerExecuteRequest;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.dag.Vertex;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

public class ExecuteApplicationProcessor implements ContainerPayLoadProcessor<Dummy> {
    private final long secondsToAwait;
    private final ExecutorContext executorContext;
    private final ApplicationMaster applicationMaster;
    private final ApplicationContext applicationContext;

    public ExecuteApplicationProcessor(ApplicationMaster applicationMaster) {
        this.applicationMaster = applicationMaster;
        this.applicationContext = applicationMaster.getApplicationContext();
        JetApplicationConfig config = this.applicationContext.getJetApplicationConfig();
        this.secondsToAwait = config.getJetSecondsToAwait();
        this.executorContext = this.applicationContext.getExecutorContext();
    }

    @Override
    public void process(Dummy payload) throws Exception {
        this.applicationMaster.registerExecution();

        startContainers();

        ApplicationTaskContext networkTaskContext =
                this.applicationContext.getExecutorContext().getNetworkTaskContext();
        ApplicationTaskContext applicationTaskContext =
                this.applicationContext.getExecutorContext().getApplicationTaskContext();

        try {
            this.executorContext.getNetworkExecutor().submitTaskContext(networkTaskContext);
            this.executorContext.getProcessingExecutor().submitTaskContext(applicationTaskContext);
        } catch (Throwable e) {
            try {
                networkTaskContext.interrupt();
            } finally {
                applicationTaskContext.interrupt();
            }

            if (e instanceof Exception) {
                throw (Exception) e;
            } else {
                throw (Error) e;
            }
        }
    }

    private void startContainers() throws Exception {
        Iterator<Vertex> iterator = this.applicationMaster.getDag().getRevertedTopologicalVertexIterator();

        while (iterator.hasNext()) {
            Vertex vertex = iterator.next();
            ProcessingContainer processingContainer = this.applicationMaster.getContainerByVertex(vertex);
            processingContainer.handleContainerRequest(new ContainerExecuteRequest()).get(this.secondsToAwait, TimeUnit.SECONDS);
        }
    }
}
