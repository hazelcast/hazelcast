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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.impl.runtime.RequestPayload;
import com.hazelcast.jet.impl.executor.StateMachineExecutor;
import com.hazelcast.jet.impl.executor.Task;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.BasicCompletableFuture;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Represents abstract state-machine
 * <p>
 * State-machines are used to provide consistency of main JET's objects and operations
 * <p>
 * All main operations under main objects are performed via state-machine's transition
 * <p>
 * For example if we ned to start vertex runner it will work like:
 * <p>
 * <pre>
 *      START__EVENT -&gt
 *      VERTEX_RUNNER_STATE_MACHINE
 *              -&gt Execution of request processor (here we can start vertex runner)
 *              -&gt transition to the next state
 *              -&gt output
 * </pre>
 * <p>
 * Transition matrix lets to prevent appearance of inconsistent state, for example
 * execution of the job before DAG has been submit
 *
 * @param <Input>  input type of stateMachine's event
 * @param <State>  current state of the stateMachine
 * @param <Output> output of the stateMachine
 */
public abstract class StateMachine
        <Input extends StateMachineEvent,
                State extends StateMachineState,
                Output extends StateMachineResponse> {
    protected final String name;
    protected volatile Output output;
    protected volatile State state = defaultState();

    private final ILogger logger;
    private final Map<State, Map<Input, State>> stateTransitionMatrix;
    private final JobContext jobContext;
    private final StateMachineEventHandler<Input> processor;
    private final BlockingQueue<RequestPayload<Input, Output>> eventsQueue =
            new LinkedBlockingDeque<>();

    protected StateMachine(String name,
                           Map<State, Map<Input, State>> stateTransitionMatrix,
                           StateMachineEventHandler<Input> processor,
                           JobContext jobContext) {
        this.name = name;
        this.processor = processor;
        this.jobContext = jobContext;
        this.stateTransitionMatrix = stateTransitionMatrix;
        this.logger = Logger.getLogger(StateMachine.class);

        if (jobContext != null) {
            getExecutor().addTask(new EventsProcessor(this.eventsQueue));
        }
    }

    protected abstract StateMachineExecutor getExecutor();

    protected abstract State defaultState();

    /**
     * @return current state of the state-Machine
     */
    public State currentState() {
        return state;
    }

    /**
     * Executes transition of the state-machine in accordance with request
     *
     * @param request corresponding request
     * @param <P>     type of the payload
     * @return awaiting future with results of transition
     */
    public <P> ICompletableFuture<Output> handleRequest(StateMachineRequest<Input, P> request) {
        BasicCompletableFuture<Output> future = new BasicCompletableFuture<>(jobContext.getNodeEngine(), logger);
        RequestPayload<Input, Output> payload =
                new RequestPayload<>(request.getEvent(), future, request.getPayload());

        if (!this.eventsQueue.offer(payload)) {
            throw new JetException("Can't add request to the stateMachine " + name);
        }

        return payload.getFuture();
    }

    protected abstract Output output(Input input, State nextState);

    /**
     * @return output of transition
     */
    public Output getOutput() {
        return output;
    }

    public JobContext getJobContext() {
        return this.jobContext;
    }

    private class EventsProcessor extends Task {
        private final Queue<RequestPayload<Input, Output>> requestsQueue;

        EventsProcessor(Queue<RequestPayload<Input, Output>> requestsQueue) {
            this.requestsQueue = requestsQueue;
        }

        @Override
        public boolean execute(BooleanHolder didWorkHolder) {
            RequestPayload<Input, Output> requestHolder = this.requestsQueue.poll();

            try {
                if (requestHolder == null) {
                    didWorkHolder.set(false);
                    return true;
                }

                Input event = requestHolder.getEvent();
                BasicCompletableFuture<Output> future = requestHolder.getFuture();

                try {
                    Map<Input, State> transmissions = stateTransitionMatrix.get(state);

                    if (transmissions == null) {
                        future.setResult(new InvalidEventException(event, state, name));
                        return true;
                    }

                    State nextState = transmissions.get(event);

                    if (nextState != null) {
                        if (processor != null) {
                            processor.handleEvent(requestHolder.getEvent(), requestHolder.getPayload());
                        }

                        if (logger.isFineEnabled()) {
                            logger.fine("Transitioned from state=" + state + " to=" + nextState + " on event " + event);
                        }
                        state = nextState;
                        output = output(event, nextState);
                        future.setResult(output);
                    } else {
                        output = output(event, null);
                        Throwable error = new InvalidEventException(event, state, name);
                        logger.warning(error.getMessage(), error);
                        future.setResult(error);
                    }
                } catch (Throwable e) {
                    if (logger != null) {
                        logger.warning(e.getMessage(), e);
                    }

                    future.setResult(e);
                }

                return true;
            } finally {
                didWorkHolder.set(this.requestsQueue.size() > 0);
            }
        }
    }
}
