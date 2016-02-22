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


import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.ContainerStateMachine;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterStateMachineFactory;
import com.hazelcast.jet.impl.statemachine.applicationmaster.ApplicationMasterStateMachineImpl;
import com.hazelcast.spi.NodeEngine;

public class DefaultApplicationMasterStateMachineFactory implements ApplicationMasterStateMachineFactory {
    @Override
    public ContainerStateMachine<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
    newStateMachine(String name,
                    StateMachineRequestProcessor<ApplicationMasterEvent> processor,
                    NodeEngine nodeEngine,
                    ApplicationContext applicationContext) {
        return new ApplicationMasterStateMachineImpl(name, processor, nodeEngine, applicationContext);
    }
}
