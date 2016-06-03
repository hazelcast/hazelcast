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

package com.hazelcast.jet.impl.statemachine.application;

import com.hazelcast.jet.impl.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.statemachine.StateMachineFactory;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.spi.NodeEngine;

/**
 * Represents factory to create application's state-machine;
 */
public interface ApplicationStateMachineFactory extends
        StateMachineFactory<ApplicationEvent, ApplicationState, ApplicationResponse> {
    /**
     * Create new state-machine;
     *
     * @param name               - name of the state-machine;
     * @param processor          - corresponding state-machine processor;
     * @param nodeEngine         - Hazelcast's nodeEngine;
     * @param applicationContext - Jet ApplicationContext;
     * @return - constructed state-machine;
     */
    ApplicationStateMachine newStateMachine(String name,
                                            StateMachineRequestProcessor<ApplicationEvent> processor,
                                            NodeEngine nodeEngine,
                                            ApplicationContext applicationContext
    );
}
