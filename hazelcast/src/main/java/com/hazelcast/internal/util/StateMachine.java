/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkState;

/**
 * Simple state machine using an Enum as possible states.
 *
 * @param <T>
 */
public class StateMachine<T extends Enum<T>> {

    private Map<T, Set<T>> transitions = new HashMap<T, Set<T>>();
    private T currentState;

    public StateMachine(T initialState) {
        currentState = initialState;
    }

    public static <T extends Enum<T>> StateMachine<T> of(T initialState) {
        return new StateMachine<T>(initialState);
    }

    /**
     * Add a valid transition from one state to one or more states
     **/
    public StateMachine<T> withTransition(T from, T to, T... moreTo) {
        transitions.put(from, EnumSet.of(to, moreTo));
        return this;
    }

    /**
     * Transition to next state
     *
     * @throws IllegalStateException if transition is not allowed
     * @throws NullPointerException  if the {@code nextState} is {@code null}
     */
    public StateMachine<T> next(T nextState) throws IllegalStateException {
        Set<T> allowed = transitions.get(currentState);
        checkNotNull(allowed, "No transitions from state " + currentState);
        checkState(allowed.contains(nextState), "Transition not allowed from state " + currentState + " to " + nextState);
        currentState = nextState;
        return this;
    }

    /**
     * Transition to next state if not already there
     *
     * @throws IllegalStateException if transition is not allowed
     * @throws NullPointerException  if the {@code nextState} is {@code null}
     */
    public void nextOrStay(T nextState) {
        if (!is(nextState)) {
            next(nextState);
        }
    }

    /**
     * Check if current state is one of given states
     */
    public boolean is(T state, T... otherStates) {
        return EnumSet.of(state, otherStates).contains(currentState);
    }

    @Override
    public String toString() {
        return "StateMachine{state=" + currentState + "}";
    }
}
