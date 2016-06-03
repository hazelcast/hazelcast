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

package com.hazelcast.jet.impl.container.applicationmaster;

import com.hazelcast.jet.impl.container.ContainerState;

/**
 * Represents state of the application master;
 */
public enum ApplicationMasterState implements ContainerState {
    /**
     * Initial state of the applicationMaster
     */
    NEW,
    /**
     * State after DAG has been submitted;
     */
    DAG_SUBMITTED,
    /**
     * State when applicationMaster is ready for execution;
     */
    READY_FOR_EXECUTION,
    /**
     * State to be set if DAG was invalid;
     */
    INVALID_DAG,
    /**
     * State to be during application's execution;
     */
    EXECUTING,
    /**
     * State during application's interrupting;
     */
    EXECUTION_INTERRUPTING,
    /**
     * State after application has been interrupted;
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
