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

package com.hazelcast.jet.internal.impl.application;


import com.hazelcast.jet.internal.api.application.ApplicationStateManager;
import com.hazelcast.jet.internal.api.statemachine.ApplicationStateMachine;
import com.hazelcast.jet.internal.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.internal.api.statemachine.application.ApplicationState;
import com.hazelcast.jet.internal.impl.statemachine.application.ApplicationStateMachineImpl;

public class DefaultApplicationStateManager implements ApplicationStateManager {
    private final ApplicationStateMachine applicationStateMachine;

    public DefaultApplicationStateManager(String applicationName) {
        this.applicationStateMachine = new ApplicationStateMachineImpl(applicationName);
    }

    public ApplicationState getApplicationState() {
        return this.applicationStateMachine.currentState();
    }

    public void onEvent(ApplicationEvent applicationEvent) {
        this.applicationStateMachine.onEvent(applicationEvent);
    }
}
