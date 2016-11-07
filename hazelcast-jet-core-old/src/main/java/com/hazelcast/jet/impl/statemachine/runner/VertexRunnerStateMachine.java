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

package com.hazelcast.jet.impl.statemachine.runner;

import com.hazelcast.jet.impl.runtime.VertexRunnerEvent;
import com.hazelcast.jet.impl.runtime.VertexRunnerResponse;
import com.hazelcast.jet.impl.runtime.VertexRunnerState;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.statemachine.StateMachine;
import com.hazelcast.jet.impl.statemachine.StateMachineEventHandler;
import com.hazelcast.jet.impl.util.LinkedMapBuilder;
import java.util.Map;

public class VertexRunnerStateMachine extends
        StateMachine<VertexRunnerEvent, VertexRunnerState, VertexRunnerResponse> {

    private static final Map<VertexRunnerState, Map<VertexRunnerEvent, VertexRunnerState>>
            STATE_TRANSITION_MATRIX =
            LinkedMapBuilder.<VertexRunnerState, Map<VertexRunnerEvent, VertexRunnerState>>builder().
                    put(
                            VertexRunnerState.NEW, LinkedMapBuilder.of(
                                    VertexRunnerEvent.START, VertexRunnerState.AWAITING,
                                    VertexRunnerEvent.FINALIZE, VertexRunnerState.FINALIZED
                            )
                    ).
                    put(
                            VertexRunnerState.AWAITING, LinkedMapBuilder.of(
                                    VertexRunnerEvent.EXECUTE, VertexRunnerState.EXECUTION,
                                    VertexRunnerEvent.FINALIZE, VertexRunnerState.FINALIZED,
                                    VertexRunnerEvent.INTERRUPTED, VertexRunnerState.AWAITING
                            )
                    ).
                    put(
                            VertexRunnerState.EXECUTION, LinkedMapBuilder.of(
                                    VertexRunnerEvent.EXECUTION_COMPLETED, VertexRunnerState.AWAITING,
                                    VertexRunnerEvent.INTERRUPT, VertexRunnerState.INTERRUPTING,
                                    VertexRunnerEvent.INTERRUPTED, VertexRunnerState.AWAITING
                            )
                    ).
                    put(
                            VertexRunnerState.INTERRUPTING, LinkedMapBuilder.of(
                                    VertexRunnerEvent.EXECUTION_COMPLETED, VertexRunnerState.AWAITING,
                                    VertexRunnerEvent.FINALIZE, VertexRunnerState.FINALIZED,
                                    VertexRunnerEvent.INTERRUPTED, VertexRunnerState.AWAITING
                            )
                    ).
                    build();

    public VertexRunnerStateMachine(String name,
                                    StateMachineEventHandler<VertexRunnerEvent> processor,
                                    JobContext jobContext) {
        super(name, STATE_TRANSITION_MATRIX, processor, jobContext);
    }

    @Override
    protected StateMachineExecutor getExecutor() {
        return getJobContext().getExecutorContext().getVertexManagerStateMachineExecutor();
    }

    @Override
    protected VertexRunnerState defaultState() {
        return VertexRunnerState.NEW;
    }

    @Override
    protected VertexRunnerResponse output(VertexRunnerEvent vertexRunnerEvent,
                                          VertexRunnerState nextState) {
        if (nextState == null) {
            return VertexRunnerResponse.FAILURE;
        } else {
            return VertexRunnerResponse.SUCCESS;
        }
    }
}
