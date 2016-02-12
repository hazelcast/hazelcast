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

import java.util.Map;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;
import com.hazelcast.jet.api.executor.TaskExecutor;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.impl.statemachine.AbstractStateMachineImpl;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.api.statemachine.application.ApplicationState;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.api.statemachine.application.ApplicationResponse;

public class ApplicationStateMachineImpl extends AbstractStateMachineImpl<ApplicationEvent, ApplicationState, ApplicationResponse>
        implements ApplicationStateMachine {

    private static final Map<ApplicationState, Map<ApplicationEvent, ApplicationState>> STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<ApplicationState, Map<ApplicationEvent, ApplicationState>>builder().
                    put(
                            ApplicationState.NEW, LinkedMapBuilder.of(
                                    ApplicationEvent.INIT_SUCCESS, ApplicationState.INIT_SUCCESS,
                                    ApplicationEvent.INIT_FAILURE, ApplicationState.INIT_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.INIT_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.LOCALIZATION_START, ApplicationState.LOCALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.LOCALIZATION_SUCCESS, ApplicationState.LOCALIZATION_SUCCESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.INIT_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.LOCALIZATION_SUCCESS, ApplicationState.LOCALIZATION_SUCCESS,
                                    ApplicationEvent.LOCALIZATION_FAILURE, ApplicationState.LOCALIZATION_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.SUBMIT_START, ApplicationState.SUBMIT_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.SUBMIT_SUCCESS, ApplicationState.SUBMIT_SUCCESS
                            )
                    ).
                    put(
                            ApplicationState.LOCALIZATION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.SUBMIT_SUCCESS, ApplicationState.SUBMIT_SUCCESS,
                                    ApplicationEvent.SUBMIT_FAILURE, ApplicationState.SUBMIT_FAILURE
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS,
                                    ApplicationEvent.EXECUTION_SUCCESS, ApplicationState.EXECUTION_SUCCESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.SUBMIT_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_SUCCESS, ApplicationState.EXECUTION_SUCCESS,
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.EXECUTION_FAILURE,
                                    ApplicationEvent.INTERRUPTION_SUCCESS, ApplicationState.INTERRUPTION_SUCCESS,
                                    ApplicationEvent.INTERRUPTION_FAILURE, ApplicationState.INTERRUPTION_FAILURE,
                                    ApplicationEvent.INTERRUPTION_START, ApplicationState.INTERRUPTION_IN_PROGRESS
                            )
                    ).
                    put(
                            ApplicationState.INTERRUPTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.INTERRUPTION_IN_PROGRESS,
                                    ApplicationEvent.INTERRUPTION_SUCCESS, ApplicationState.INTERRUPTION_SUCCESS
                            )
                    ).
                    put(
                            ApplicationState.INTERRUPTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS,
                                    ApplicationEvent.EXECUTION_FAILURE, ApplicationState.INTERRUPTION_SUCCESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    ApplicationEvent.EXECUTION_START, ApplicationState.EXECUTION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.EXECUTION_SUCCESS, ApplicationState.EXECUTION_SUCCESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.EXECUTION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.FINALIZATION_IN_PROGRESS, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_FAILURE, ApplicationState.FINALIZATION_FAILURE,
                                    ApplicationEvent.FINALIZATION_SUCCESS , ApplicationState.NEW
                            )
                    ).
                    put(
                            ApplicationState.FINALIZATION_FAILURE, LinkedMapBuilder.of(
                                    ApplicationEvent.FINALIZATION_START, ApplicationState.FINALIZATION_IN_PROGRESS,
                                    ApplicationEvent.FINALIZATION_SUCCESS, ApplicationState.NEW
                            )
                    ).build();

    public ApplicationStateMachineImpl(String name,
                                       StateMachineRequestProcessor<ApplicationEvent> processor,
                                       NodeEngine nodeEngine,
                                       ApplicationContext applicationContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, nodeEngine, applicationContext);
    }

    public ApplicationStateMachineImpl(String name) {
        super(name, STATE_TRANSITION_MATRIX, null, null, null);
    }

    @Override
    protected ApplicationResponse output(ApplicationEvent applicationEvent, ApplicationState nextState) {
        if (nextState == null) {
            return ApplicationResponse.FAILURE;
        } else {
            return ApplicationResponse.SUCCESS;
        }
    }

    @Override
    protected TaskExecutor getExecutor() {
        return getApplicationContext().getExecutorContext().getApplicationStateMachineExecutor();
    }

    @Override
    protected ApplicationState defaultState() {
        return ApplicationState.NEW;
    }

    @Override
    public void onEvent(ApplicationEvent applicationEvent) {
        Map<ApplicationEvent, ApplicationState> transition = STATE_TRANSITION_MATRIX.get(currentState());

        if (transition != null) {
            ApplicationState state = transition.get(applicationEvent);

            if (state == null) {
                raise(applicationEvent);
            }

            this.output = output(applicationEvent, state);
            this.state = state;
        } else {
            raise(applicationEvent);
        }
    }

    private void raise(ApplicationEvent applicationEvent) {
        throw new IllegalStateException(
                "Invalid event "
                        + applicationEvent
                        + " currentState="
                        + this.state
        );
    }
}

