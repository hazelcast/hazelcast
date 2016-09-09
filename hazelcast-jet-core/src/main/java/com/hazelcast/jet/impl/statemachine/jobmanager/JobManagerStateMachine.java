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

package com.hazelcast.jet.impl.statemachine.jobmanager;

import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerEvent;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerResponse;
import com.hazelcast.jet.impl.runtime.jobmanager.JobManagerState;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineRequestProcessor;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;

import java.util.Map;

public class JobManagerStateMachine extends StateMachine<JobManagerEvent, JobManagerState, JobManagerResponse> {

    private static final Map<JobManagerState, Map<JobManagerEvent, JobManagerState>>
            STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<JobManagerState, Map<JobManagerEvent, JobManagerState>>builder().
                    put(
                            JobManagerState.NEW, LinkedMapBuilder.of(
                                    JobManagerEvent.SUBMIT_DAG, JobManagerState.DAG_SUBMITTED,
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED
                            )
                    ).
                    put(
                            JobManagerState.DAG_SUBMITTED, LinkedMapBuilder.of(
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED,
                                    JobManagerEvent.EXECUTION_PLAN_READY, JobManagerState.READY_FOR_EXECUTION
                            )
                    ).
                    put(
                            JobManagerState.READY_FOR_EXECUTION, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTE, JobManagerState.EXECUTING
                            )
                    ).
                    put(
                            JobManagerState.EXECUTING, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTION_ERROR, JobManagerState.EXECUTION_FAILED,
                                    JobManagerEvent.EXECUTION_COMPLETED, JobManagerState.EXECUTION_SUCCESS,
                                    JobManagerEvent.INTERRUPT_EXECUTION, JobManagerState.EXECUTION_INTERRUPTING
                            )
                    ).
                    put(
                            JobManagerState.EXECUTION_SUCCESS, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTE, JobManagerState.EXECUTING,
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED,
                                    JobManagerEvent.INTERRUPT_EXECUTION, JobManagerState.EXECUTION_SUCCESS
                            )
                    ).
                    put(
                            JobManagerState.EXECUTION_FAILED, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTION_COMPLETED, JobManagerState.EXECUTION_FAILED,
                                    JobManagerEvent.EXECUTE, JobManagerState.EXECUTING,
                                    JobManagerEvent.EXECUTION_ERROR, JobManagerState.EXECUTION_FAILED,
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED,
                                    JobManagerEvent.EXECUTION_INTERRUPTED, JobManagerState.EXECUTION_FAILED
                            )
                    ).
                    put(
                            JobManagerState.EXECUTION_INTERRUPTING, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTION_INTERRUPTED, JobManagerState.EXECUTION_INTERRUPTED
                            )
                    ).
                    put(
                            JobManagerState.EXECUTION_INTERRUPTED, LinkedMapBuilder.of(
                                    JobManagerEvent.EXECUTION_ERROR, JobManagerState.EXECUTION_INTERRUPTED,
                                    JobManagerEvent.EXECUTE, JobManagerState.EXECUTING,
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED
                            )
                    ).
                    put(
                            JobManagerState.INVALID_DAG, LinkedMapBuilder.of(
                                    JobManagerEvent.FINALIZE, JobManagerState.FINALIZED
                            )
                    ).build();

    public JobManagerStateMachine(String name,
                                  StateMachineRequestProcessor<JobManagerEvent> processor,
                                  JobContext jobContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, jobContext);
    }

    @Override
    protected StateMachineExecutor getExecutor() {
        return getJobContext().getExecutorContext().getJobManagerStateMachineExecutor();
    }

    @Override
    protected JobManagerState defaultState() {
        return JobManagerState.NEW;
    }

    @Override
    protected JobManagerResponse output(JobManagerEvent jobManagerEvent, JobManagerState nextState) {
        if (nextState == null) {
            return JobManagerResponse.FAILURE;
        } else {
            return JobManagerResponse.SUCCESS;
        }
    }
}
