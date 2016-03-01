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

package com.hazelcast.jet.api.statemachine;

import java.util.concurrent.Future;

/**
 * Represents abstract state-machine;
 *
 * State-machines are used to provide consistency of main JET's objects and operations;
 *
 * All main operations under main objects are performed via state-machine's transition;
 *
 * For example if we ned to start container it will work like:
 *
 * <pre>
 *      START__EVENT -&gt;
 *      CONTAINER_STATE_MACHINE
 *              -&gt; Execution of request processor (here we can start container)
 *              -&gt; transition to the next state
 *              -&gt; output
 * </pre>
 *
 * Transition matrix lets to prevent appearance of inconsistent state, for example
 * execution of the application before DAG has been submit;
 *
 * @param <Input>  - input type of stateMachine's event;
 * @param <State>  - current state of the stateMachine;
 * @param <Output> - output of the stateMachine;
 */
public interface StateMachine
        <Input extends StateMachineEvent,
                State extends StateMachineState,
                Output extends StateMachineOutput> {
    /**
     * @return - current state of the state-Machine;
     */
    State currentState();

    /**
     * @return - output of transition;
     */
    Output getOutput();

    /**
     * Executes transition of the state-machine in accordance with request;
     *
     * @param request - corresponding request;
     * @param <P>     - type of the payLoad;
     * @return - awaiting future with results of transition;
     */
    <P> Future<Output> handleRequest(StateMachineRequest<Input, P> request);
}

