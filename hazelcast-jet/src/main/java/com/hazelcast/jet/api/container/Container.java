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

package com.hazelcast.jet.api.container;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.StateMachine;
import com.hazelcast.jet.api.statemachine.container.ContainerEvent;
import com.hazelcast.jet.api.statemachine.container.ContainerState;
import com.hazelcast.spi.NodeEngine;

import java.util.List;

/**
 * Interface which represents abstract JET-container;
 * Containers:
 * <p/>
 * <pre>
 *     1) Application master;
 *     2) Data processing container;
 * </pre>
 *
 * @param <SI> - type of the input container state-machine event;
 * @param <SS> - type of the  container's state;
 * @param <SO> - type of the output;
 */
public interface Container
        <SI extends ContainerEvent,
                SS extends ContainerState,
                SO extends ContainerResponse> extends
        ContainerRequestHandler<SI, SO>,
        ContainerStateMachineRequestProcessor<SI> {
    /**
     * @return - Hazelcast nodeEngine object;
     */
    NodeEngine getNodeEngine();

    /**
     * @return - corresponding container state-machine;
     */
    StateMachine<SI, SS, SO> getStateMachine();

    /**
     * @return - JET-application context;
     */
    ApplicationContext getApplicationContext();

    /**
     * Register followed container;
     *
     * @param container - followed container;
     */
    void addFollower(ProcessingContainer container);

    /**
     * Register previous container;
     *
     * @param container - corresnponding container;
     */
    void addPredecessor(ProcessingContainer container);

    /**
     * @return - list of containers followed by current container;
     */
    List<ProcessingContainer> getFollowers();

    /**
     * @return - list of previous containers;
     */
    List<ProcessingContainer> getPredecessors();

    /**
     * @return - context of container;
     */
    ContainerContext getContainerContext();

    /**
     * @return - container's identifier;
     */
    int getID();
}
