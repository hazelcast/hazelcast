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

package com.hazelcast.jet.impl.statemachine.applicationmaster;

import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.executor.TaskExecutor;
import com.hazelcast.jet.api.statemachine.AppMasterStateMachine;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterEvent;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterResponse;
import com.hazelcast.jet.api.statemachine.container.applicationmaster.ApplicationMasterState;
import com.hazelcast.jet.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;

public class ApplicationMasterStateMachineImpl extends
        AbstractStateMachineImpl<ApplicationMasterEvent, ApplicationMasterState, ApplicationMasterResponse>
        implements AppMasterStateMachine {

    private static final Map<ApplicationMasterState, Map<ApplicationMasterEvent, ApplicationMasterState>>
            STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<ApplicationMasterState, Map<ApplicationMasterEvent, ApplicationMasterState>>builder().
                    put(
                            ApplicationMasterState.NEW, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.SUBMIT_DAG, ApplicationMasterState.DAG_SUBMITTED,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED
                            )
                    ).
                    put(
                            ApplicationMasterState.DAG_SUBMITTED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.EXECUTION_PLAN_READY, ApplicationMasterState.READY_FOR_EXECUTION,
                                    ApplicationMasterEvent.EXECUTION_PLAN_BUILD_FAILED, ApplicationMasterState.INVALID_DAG
                            )
                    ).
                    put(
                            ApplicationMasterState.READY_FOR_EXECUTION, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTING, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTION_ERROR, ApplicationMasterState.EXECUTION_FAILED,
                                    ApplicationMasterEvent.EXECUTION_COMPLETED, ApplicationMasterState.EXECUTION_SUCCESS,
                                    ApplicationMasterEvent.INTERRUPT_EXECUTION, ApplicationMasterState.EXECUTION_INTERRUPTING
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_FAILED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTION_COMPLETED, ApplicationMasterState.EXECUTION_FAILED,
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.EXECUTION_ERROR, ApplicationMasterState.EXECUTION_FAILED,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED,
                                    ApplicationMasterEvent.EXECUTION_INTERRUPTED, ApplicationMasterState.EXECUTION_FAILED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_INTERRUPTING, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTION_INTERRUPTED, ApplicationMasterState.EXECUTION_INTERRUPTED
                            )
                    ).
                    put(
                            ApplicationMasterState.EXECUTION_INTERRUPTED, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.EXECUTION_ERROR, ApplicationMasterState.EXECUTION_INTERRUPTED,
                                    ApplicationMasterEvent.EXECUTE, ApplicationMasterState.EXECUTING,
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED
                            )
                    ).
                    put(
                            ApplicationMasterState.INVALID_DAG, LinkedMapBuilder.of(
                                    ApplicationMasterEvent.FINALIZE, ApplicationMasterState.FINALIZED
                            )
                    ).build();

    public ApplicationMasterStateMachineImpl(String name,
                                             StateMachineRequestProcessor<ApplicationMasterEvent> processor,
                                             NodeEngine nodeEngine,
                                             ApplicationContext applicationContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, nodeEngine, applicationContext);
    }

    @Override
    protected TaskExecutor getExecutor() {
        return getApplicationContext().getExecutorContext().getApplicationMasterStateMachineExecutor();
    }

    @Override
    protected ApplicationMasterState defaultState() {
        return ApplicationMasterState.NEW;
    }

    @Override
    protected ApplicationMasterResponse output(ApplicationMasterEvent applicationMasterEvent, ApplicationMasterState nextState) {
        if (nextState == null) {
            return ApplicationMasterResponse.FAILURE;
        } else {
            return ApplicationMasterResponse.SUCCESS;
        }
    }
}
