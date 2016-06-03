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

import com.hazelcast.jet.impl.container.ContainerEvent;

/**
 * Represents event for the applicationMaster;
 */
public enum ApplicationMasterEvent implements ContainerEvent {
    /**
     * Submit-dag event;
     */
    SUBMIT_DAG,
    /**
     * Sent on execution plan building failure;
     */
    EXECUTION_PLAN_BUILD_FAILED,
    /**
     * Sent on execution plan ready;
     */
    EXECUTION_PLAN_READY,
    /**
     * Command to execute application;
     */
    EXECUTE,
    /**
     * Command to interrupt application;
     */
    INTERRUPT_EXECUTION,
    /**
     * Sent on execution interrupted;
     */
    EXECUTION_INTERRUPTED,
    /**
     * Sent on execution error;
     */
    EXECUTION_ERROR,
    /**
     * Sent on execution completion;
     */
    EXECUTION_COMPLETED,
    /**
     * Command to finalize application;
     */
    FINALIZE,
}
