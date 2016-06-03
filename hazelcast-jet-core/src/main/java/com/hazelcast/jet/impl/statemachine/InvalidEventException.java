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

package com.hazelcast.jet.impl.statemachine;

/**
 * Raised in case of invalid transition of the stateMachine;
 */
public class InvalidEventException extends Exception {
    private final Object event;
    private final Object state;

    public InvalidEventException(Object event, Object state, String name) {
        super("Invalid Event " + event + " for state=" + state + " for stateMachine with name=" + name);
        this.event = event;
        this.state = state;
    }

    /**
     * @return - current state of the state-machine;
     */
    public Object getState() {
        return state;
    }

    /**
     * @return - event which raised transition of the state-machine;
     */
    public Object getEvent() {
        return event;
    }
}
