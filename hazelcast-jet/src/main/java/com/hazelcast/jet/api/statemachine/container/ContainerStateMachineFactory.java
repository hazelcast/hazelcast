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

package com.hazelcast.jet.api.statemachine.container;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.container.ContainerResponse;
import com.hazelcast.jet.api.statemachine.StateMachineFactory;
import com.hazelcast.jet.api.statemachine.ContainerStateMachine;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;

/**
 * Represents factory to construct stateMachine of the containers;
 *
 * @param <Input>  - type of the input events;
 * @param <State>  - type of the state-machine  internal states;
 * @param <Output> - type of the output;
 */
public interface ContainerStateMachineFactory
        <Input extends ContainerEvent,
                State extends ContainerState,
                Output extends ContainerResponse>
        extends StateMachineFactory<Input, State, Output> {
    /**
     * Constructs and return new container stateMachine;
     *
     * @param name               - name of the stateMachine;
     * @param processor          - stateMachine processor;
     * @param nodeEngine         - Hazelcast nodeEngine;
     * @param applicationContext - applicationContext;
     * @return - corresponding container stateMachine;
     */
    ContainerStateMachine<Input, State, Output> newStateMachine(String name,
                                                                StateMachineRequestProcessor<Input> processor,
                                                                NodeEngine nodeEngine,
                                                                ApplicationContext applicationContext
    );
}
