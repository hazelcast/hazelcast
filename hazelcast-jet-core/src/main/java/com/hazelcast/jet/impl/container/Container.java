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

package com.hazelcast.jet.impl.container;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineEvent;
import com.hazelcast.jet.impl.statemachine.StateMachineFactory;
import com.hazelcast.jet.impl.statemachine.StateMachineRequest;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.statemachine.StateMachineResponse;
import com.hazelcast.jet.impl.statemachine.StateMachineState;
import com.hazelcast.spi.NodeEngine;

/**
 * Interface which represents abstract JET-container
 * Containers:
 * <p/>
 * <pre>
 *     1) Job Manager
 *     2) Processing container
 * </pre>
 *
 * @param <SI> type of the input container state-machine event
 * @param <SS> type of the  container's state
 * @param <SO> type of the output
 */
public abstract class Container
        <SI extends StateMachineEvent,
                SS extends StateMachineState,
                SO extends StateMachineResponse>
        implements StateMachineRequestProcessor<SI> {
    private final int id;

    private final NodeEngine nodeEngine;
    private final ContainerContextImpl containerContext;
    private final JobContext jobContext;
    private final StateMachine<SI, SS, SO> stateMachine;

    protected Container(StateMachineFactory<SI, StateMachine<SI, SS, SO>> stateMachineFactory,
                        NodeEngine nodeEngine, JobContext jobContext
    ) {
        this(null, stateMachineFactory, nodeEngine, jobContext);
    }

    protected Container(Vertex vertex, StateMachineFactory<SI, StateMachine<SI, SS, SO>> stateMachineFactory,
                        NodeEngine nodeEngine, JobContext jobContext
    ) {
        this.nodeEngine = nodeEngine;
        String name = vertex == null ? jobContext.getName() : vertex.getName();
        this.stateMachine = stateMachineFactory.newStateMachine(name, this, nodeEngine, jobContext);
        this.jobContext = jobContext;
        this.id = jobContext.getContainerIDGenerator().incrementAndGet();
        this.containerContext = new ContainerContextImpl(nodeEngine, jobContext, this.id, vertex);
    }

    /**
         * @return Hazelcast nodeEngine object
         */
    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
         * Handle's container's request with state-machine's input event
         *
         * @param request corresponding request
         * @param <P>   type of request payload
         * @return awaiting future
         */
    public <P> ICompletableFuture<SO> handleContainerRequest(StateMachineRequest<SI, P> request) {
        try {
            return stateMachine.handleRequest(request);
        } finally {
            wakeUpExecutor();
        }
    }

    protected abstract void wakeUpExecutor();

    /**
         * @return JET-job context
         */
    public JobContext getJobContext() {
        return jobContext;
    }

    /**
         * @return context of container
         */
    public ContainerContextImpl getContainerContext() {
        return containerContext;
    }

    /**
         * @return container's identifier
         */
    public int getID() {
        return id;
    }
}
