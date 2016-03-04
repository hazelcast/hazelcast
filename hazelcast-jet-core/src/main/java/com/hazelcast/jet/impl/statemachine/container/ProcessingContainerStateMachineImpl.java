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

package com.hazelcast.jet.impl.statemachine.container;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.executor.TaskExecutor;
import com.hazelcast.jet.api.statemachine.ProcessingContainerStateMachine;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerEvent;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerResponse;
import com.hazelcast.jet.api.statemachine.container.processingcontainer.ProcessingContainerState;
import com.hazelcast.jet.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;

public class ProcessingContainerStateMachineImpl extends
        AbstractStateMachineImpl<ProcessingContainerEvent, ProcessingContainerState, ProcessingContainerResponse>
        implements ProcessingContainerStateMachine {
    private static final Map<ProcessingContainerState, Map<ProcessingContainerEvent, ProcessingContainerState>>
            STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<ProcessingContainerState, Map<ProcessingContainerEvent, ProcessingContainerState>>builder().
                    put(
                            ProcessingContainerState.NEW, LinkedMapBuilder.of(
                                    ProcessingContainerEvent.START, ProcessingContainerState.AWAITING,
                                    ProcessingContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED
                            )
                    ).
                    put(
                            ProcessingContainerState.AWAITING, LinkedMapBuilder.of(
                                    ProcessingContainerEvent.EXECUTE, ProcessingContainerState.EXECUTION,
                                    ProcessingContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED,
                                    ProcessingContainerEvent.INTERRUPTED, ProcessingContainerState.AWAITING
                            )
                    ).
                    put(
                            ProcessingContainerState.EXECUTION, LinkedMapBuilder.of(
                                    ProcessingContainerEvent.EXECUTION_COMPLETED, ProcessingContainerState.AWAITING,
                                    ProcessingContainerEvent.INTERRUPT, ProcessingContainerState.INTERRUPTING,
                                    ProcessingContainerEvent.INTERRUPTED, ProcessingContainerState.AWAITING
                            )
                    ).
                    put(
                            ProcessingContainerState.INTERRUPTING, LinkedMapBuilder.of(
                                    ProcessingContainerEvent.EXECUTION_COMPLETED, ProcessingContainerState.AWAITING,
                                    ProcessingContainerEvent.FINALIZE, ProcessingContainerState.FINALIZED,
                                    ProcessingContainerEvent.INTERRUPTED, ProcessingContainerState.AWAITING
                            )
                    ).
                    build();

    public ProcessingContainerStateMachineImpl(String name,
                                               StateMachineRequestProcessor<ProcessingContainerEvent> processor,
                                               NodeEngine nodeEngine,
                                               ApplicationContext applicationContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, nodeEngine, applicationContext);
    }

    @Override
    protected TaskExecutor getExecutor() {
        return getApplicationContext().getExecutorContext().getDataContainerStateMachineExecutor();
    }

    @Override
    protected ProcessingContainerState defaultState() {
        return ProcessingContainerState.NEW;
    }

    @Override
    protected ProcessingContainerResponse output(ProcessingContainerEvent processingContainerEvent,
                                                 ProcessingContainerState nextState) {
        if (nextState == null) {
            return ProcessingContainerResponse.FAILURE;
        } else {
            return ProcessingContainerResponse.SUCCESS;
        }
    }
}
