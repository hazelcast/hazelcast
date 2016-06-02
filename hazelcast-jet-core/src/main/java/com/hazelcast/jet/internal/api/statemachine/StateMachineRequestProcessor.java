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

package com.hazelcast.jet.internal.api.statemachine;

/**
 * Represents abstract stateMachine processor to execute transitions;
 *
 * @param <SI> - type of the input stateMachine event;
 */
public interface StateMachineRequestProcessor<SI extends StateMachineEvent> {
    /**
     * Will be executed on next transition request;
     *
     * @param event   - corresponding input event;
     * @param payLoad - arguments payLoad;
     * @throws Exception if any exception
     */
    void processRequest(SI event, Object payLoad) throws Exception;
}
