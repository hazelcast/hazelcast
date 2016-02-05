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

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Future;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.api.container.Container;
import com.hazelcast.jet.api.container.ProcessingContainer;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerResponse;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.ContainerStateMachine;
import com.hazelcast.jet.api.statemachine.container.ContainerState;
import com.hazelcast.jet.api.statemachine.container.ContainerEvent;
import com.hazelcast.jet.api.statemachine.container.ContainerRequest;
import com.hazelcast.jet.impl.processor.context.DefaultContainerContext;
import com.hazelcast.jet.api.statemachine.container.ContainerStateMachineFactory;

public abstract class AbstractContainer
        <SI extends ContainerEvent,
                SS extends ContainerState,
                SO extends ContainerResponse>
        implements Container<SI, SS, SO> {
    private final int id;

    private final NodeEngine nodeEngine;
    private final List<ProcessingContainer> followers;
    private final List<ProcessingContainer> predecessors;
    private final ContainerContext containerContext;
    private final ApplicationContext applicationContext;
    private final ContainerStateMachine<SI, SS, SO> stateMachine;

    public AbstractContainer(ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory,
                             NodeEngine nodeEngine,
                             ApplicationContext applicationContext,
                             TupleFactory tupleFactory) {
        this(null, stateMachineFactory, nodeEngine, applicationContext, tupleFactory);
    }

    public AbstractContainer(Vertex vertex,
                             ContainerStateMachineFactory<SI, SS, SO> stateMachineFactory,
                             NodeEngine nodeEngine,
                             ApplicationContext applicationContext,
                             TupleFactory tupleFactory) {
        this.nodeEngine = nodeEngine;
        String name = vertex == null ? applicationContext.getName() : vertex.getName();
        this.stateMachine = stateMachineFactory.newStateMachine(name, this, nodeEngine, applicationContext);
        this.applicationContext = applicationContext;
        this.followers = new ArrayList<ProcessingContainer>();
        this.predecessors = new ArrayList<ProcessingContainer>();
        this.id = applicationContext.getContainerIDGenerator().incrementAndGet();
        this.containerContext = new DefaultContainerContext(nodeEngine, applicationContext, this.id, vertex, tupleFactory);
    }

    @Override
    public NodeEngine getNodeEngine() {
        return this.nodeEngine;
    }

    @Override
    public ContainerStateMachine<SI, SS, SO> getStateMachine() {
        return this.stateMachine;
    }

    @Override
    public void addFollower(ProcessingContainer container) {
        this.followers.add(container);
    }

    @Override
    public void addPredecessor(ProcessingContainer container) {
        this.predecessors.add(container);
    }

    @Override
    public <P> Future<SO> handleContainerRequest(ContainerRequest<SI, P> request) {
        try {
            return this.stateMachine.handleRequest(request);
        } finally {
            wakeUpExecutor();
        }
    }

    protected abstract void wakeUpExecutor();

    @Override
    public List<ProcessingContainer> getFollowers() {
        return this.followers;
    }

    @Override
    public List<ProcessingContainer> getPredecessors() {
        return this.predecessors;
    }

    @Override
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public ContainerContext getContainerContext() {
        return containerContext;
    }

    public String getApplicationName() {
        return applicationContext.getName();
    }

    @Override
    public int getID() {
        return id;
    }
}
