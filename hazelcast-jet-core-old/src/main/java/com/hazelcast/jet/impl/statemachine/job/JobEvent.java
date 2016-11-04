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

import com.hazelcast.jet.impl.statemachine.StateMachineEvent;

/**
 * Enum which represents job's state-machine events;
 * Sent over cluster to the corresponding state-machines;
 */
public enum JobEvent implements StateMachineEvent {
    /**
     * Will be sent on success of the job init operation;
     */
    INIT_SUCCESS,

    /**
     * Will be sent on failure of the job init operation;
     */
    INIT_FAILURE,

    /**
     * Will be sent on start of the deployment operation;
     */
    DEPLOYMENT_START,

    /**
     * Will be sent on success of the deployment operation;
     */
    DEPLOYMENT_SUCCESS,

    /**
     * Will be sent on failure of the deployment operation;
     */
    DEPLOYMENT_FAILURE,

    /**
     * Will be sent before DAG's submit operation;
     */
    SUBMIT_START,

    /**
     * Will be sent on DAG's-submit success;
     */
    SUBMIT_SUCCESS,

    /**
     * Will be sent on DAG's-submit failure;
     */
    SUBMIT_FAILURE,

    /**
     * Will be sent before start of the job's interruption;
     */
    INTERRUPTION_START,

    /**
     * Will be sent on interruption success;
     */
    INTERRUPTION_SUCCESS,

    /**
     * Will be sent on interruption failure;
     */
    INTERRUPTION_FAILURE,

    /**
     * Will be sent on execution start;
     */
    EXECUTION_START,

    /**
     * Will be sent on execution success;
     */
    EXECUTION_SUCCESS,

    /**
     * Will be sent on execution failure;
     */
    EXECUTION_FAILURE,

    /**
     * Will be sent on finalization start;
     */
    FINALIZATION_START,

    /**
     * Will be sent on finalization success;
     */
    FINALIZATION_SUCCESS,

    /**
     * Will be sent on finalization failure;
     */
    FINALIZATION_FAILURE
}
