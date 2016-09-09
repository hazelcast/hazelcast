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

package com.hazelcast.jet.impl.statemachine.job;

import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;
import java.util.Map;

public class JobStateMachine
        extends StateMachine<JobEvent, JobState, JobResponse> {

    private static final Map<JobState, Map<JobEvent, JobState>> STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<JobState, Map<JobEvent, JobState>>builder().
                    put(
                            JobState.NEW, LinkedMapBuilder.of(
                                    JobEvent.INIT_SUCCESS, JobState.INIT_SUCCESS,
                                    JobEvent.INIT_FAILURE, JobState.INIT_FAILURE
                            )
                    ).
                    put(
                            JobState.INIT_SUCCESS, LinkedMapBuilder.of(
                                    JobEvent.DEPLOYMENT_START, JobState.DEPLOYMENT_IN_PROGRESS,
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.DEPLOYMENT_SUCCESS, JobState.DEPLOYMENT_SUCCESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.INIT_FAILURE, LinkedMapBuilder.of(
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.DEPLOYMENT_IN_PROGRESS, LinkedMapBuilder.of(
                                    JobEvent.DEPLOYMENT_SUCCESS, JobState.DEPLOYMENT_SUCCESS,
                                    JobEvent.DEPLOYMENT_FAILURE, JobState.DEPLOYMENT_FAILURE
                            )
                    ).
                    put(
                            JobState.DEPLOYMENT_SUCCESS, LinkedMapBuilder.of(
                                    JobEvent.SUBMIT_START, JobState.SUBMIT_IN_PROGRESS,
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.SUBMIT_SUCCESS, JobState.SUBMIT_SUCCESS
                            )
                    ).
                    put(
                            JobState.DEPLOYMENT_FAILURE, LinkedMapBuilder.of(
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.SUBMIT_IN_PROGRESS, LinkedMapBuilder.of(
                                    JobEvent.SUBMIT_SUCCESS, JobState.SUBMIT_SUCCESS,
                                    JobEvent.SUBMIT_FAILURE, JobState.SUBMIT_FAILURE
                            )
                    ).
                    put(
                            JobState.SUBMIT_SUCCESS, LinkedMapBuilder.of(
                                    JobEvent.EXECUTION_START, JobState.EXECUTION_IN_PROGRESS,
                                    JobEvent.EXECUTION_SUCCESS, JobState.EXECUTION_SUCCESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.SUBMIT_FAILURE, LinkedMapBuilder.of(
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.EXECUTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    JobEvent.EXECUTION_SUCCESS, JobState.EXECUTION_SUCCESS,
                                    JobEvent.EXECUTION_FAILURE, JobState.EXECUTION_FAILURE,
                                    JobEvent.INTERRUPTION_SUCCESS, JobState.INTERRUPTION_SUCCESS,
                                    JobEvent.INTERRUPTION_FAILURE, JobState.EXECUTION_FAILURE,
                                    JobEvent.INTERRUPTION_START, JobState.INTERRUPTION_IN_PROGRESS
                            )
                    ).
                    put(
                            JobState.INTERRUPTION_IN_PROGRESS, LinkedMapBuilder.of(
                                    JobEvent.EXECUTION_SUCCESS, JobState.EXECUTION_SUCCESS,
                                    JobEvent.EXECUTION_FAILURE, JobState.EXECUTION_FAILURE,
                                    JobEvent.INTERRUPTION_SUCCESS, JobState.INTERRUPTION_SUCCESS,
                                    JobEvent.INTERRUPTION_FAILURE, JobState.EXECUTION_FAILURE
                            )
                    ).
                    put(
                            JobState.INTERRUPTION_SUCCESS, LinkedMapBuilder.of(
                                    JobEvent.EXECUTION_START, JobState.EXECUTION_IN_PROGRESS,
                                    JobEvent.EXECUTION_FAILURE, JobState.INTERRUPTION_SUCCESS,
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    JobEvent.EXECUTION_START, JobState.EXECUTION_IN_PROGRESS,
                                    JobEvent.INTERRUPTION_START, JobState.EXECUTION_SUCCESS,
                                    JobEvent.INTERRUPTION_SUCCESS, JobState.EXECUTION_SUCCESS,
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.EXECUTION_SUCCESS, JobState.EXECUTION_SUCCESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.EXECUTION_FAILURE, LinkedMapBuilder.of(
                                    JobEvent.INTERRUPTION_SUCCESS, JobState.EXECUTION_FAILURE,
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.FINALIZATION_IN_PROGRESS, LinkedMapBuilder.of(
                                    JobEvent.FINALIZATION_FAILURE, JobState.FINALIZATION_FAILURE,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).
                    put(
                            JobState.FINALIZATION_FAILURE, LinkedMapBuilder.of(
                                    JobEvent.FINALIZATION_START, JobState.FINALIZATION_IN_PROGRESS,
                                    JobEvent.FINALIZATION_SUCCESS, JobState.NEW
                            )
                    ).build();

    public JobStateMachine(String name,
                           StateMachineRequestProcessor<JobEvent> processor,
                           JobContext jobContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, jobContext);
    }

    public JobStateMachine(String name) {
        super(name, STATE_TRANSITION_MATRIX, null, null);
    }

    @Override
    protected JobResponse output(JobEvent jobEvent, JobState nextState) {
        return nextState == null ? JobResponse.FAILURE : JobResponse.SUCCESS;
    }

    @Override
    protected StateMachineExecutor getExecutor() {
        return getJobContext().getExecutorContext().getJobStateMachineExecutor();
    }

    @Override
    protected JobState defaultState() {
        return JobState.NEW;
    }

    /**
     * Invoked on next state-machine event
     *
     * @param jobEvent - corresponding state-machine event
     */
    public synchronized void onEvent(JobEvent jobEvent) {
        Map<JobEvent, JobState> transition = STATE_TRANSITION_MATRIX.get(currentState());

        if (transition != null) {
            JobState state = transition.get(jobEvent);

            if (state == null) {
                raise(jobEvent);
            }

            this.output = output(jobEvent, state);
            this.state = state;
        } else {
            raise(jobEvent);
        }
    }

    private void raise(JobEvent jobEvent) {
        throw new IllegalStateException(
                "Invalid event "
                        + jobEvent
                        + " currentState="
                        + this.state
        );
    }
}

