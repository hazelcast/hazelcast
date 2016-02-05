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

package com.hazelcast.jet.api.statemachine.application;

import com.hazelcast.jet.api.statemachine.StateMachineState;

public enum ApplicationState implements StateMachineState {
    NEW,
    INIT_IN_PROGRESS,
    INIT_SUCCESS,
    INIT_FAILURE,
    LOCALIZATION_IN_PROGRESS,
    LOCALIZATION_SUCCESS,
    LOCALIZATION_FAILURE,
    SUBMIT_IN_PROGRESS,
    SUBMIT_SUCCESS,
    SUBMIT_FAILURE,
    INTERRUPTION_IN_PROGRESS,
    INTERRUPTION_SUCCESS,
    INTERRUPTION_FAILURE,
    EXECUTION_IN_PROGRESS,
    EXECUTION_SUCCESS,
    EXECUTION_FAILURE,
    FINALIZATION_IN_PROGRESS,
    FINALIZATION_FAILURE
}
