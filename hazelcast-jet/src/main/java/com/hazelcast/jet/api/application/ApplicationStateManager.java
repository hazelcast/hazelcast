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

package com.hazelcast.jet.api.application;

import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.api.statemachine.application.ApplicationState;

/**
 * Abstract entity to manage application state machine
 */
public interface ApplicationStateManager {
    /**
     * @return state of the application state machine
     */
    ApplicationState getApplicationState();

    /**
     * Will be invoked on some application event
     *
     * @param applicationEvent - event of the application
     */
    void onEvent(ApplicationEvent applicationEvent);
}

