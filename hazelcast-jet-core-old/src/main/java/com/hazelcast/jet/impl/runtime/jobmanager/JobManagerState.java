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

package com.hazelcast.jet.impl.runtime.jobmanager;

import com.hazelcast.jet.impl.statemachine.StateMachineState;

/**
 * Represents state of the job manager;
 */
public enum JobManagerState implements StateMachineState {
    /**
     * Initial state of the job manager
     */
    NEW,
    /**
     * State after DAG has been submitted;
     */
    DAG_SUBMITTED,
    /**
     * State when job manager is ready for execution;
     */
    READY_FOR_EXECUTION,
    /**
     * State to be set if DAG was invalid;
     */
    INVALID_DAG,
    /**
     * State to be during job's execution;
     */
    EXECUTING,
    /**
     * State during job's interrupting;
     */
    EXECUTION_INTERRUPTING,
    /**
     * State after job has been interrupted;
     */
    EXECUTION_INTERRUPTED,
    /**
     * State after execution has been failed;
     */
    EXECUTION_FAILED,
    /**
     * State on execution success;
     */
    EXECUTION_SUCCESS,
    /**
     * State on execution has been finalized;
     */
    FINALIZED
}
