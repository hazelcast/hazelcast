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

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Future;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.logging.ILogger;

import java.util.concurrent.BlockingQueue;

import com.hazelcast.jet.impl.util.SettableFuture;
import com.hazelcast.jet.api.executor.Payload;

import java.util.concurrent.LinkedBlockingDeque;

import com.hazelcast.jet.api.executor.TaskExecutor;
import com.hazelcast.jet.impl.container.RequestPayLoad;
import com.hazelcast.jet.api.statemachine.StateMachine;
import com.hazelcast.jet.impl.container.task.AbstractTask;
import com.hazelcast.jet.api.application.ApplicationContext;
import com.hazelcast.jet.api.statemachine.StateMachineEvent;
import com.hazelcast.jet.api.statemachine.StateMachineState;
import com.hazelcast.jet.api.statemachine.StateMachineOutput;
import com.hazelcast.jet.api.statemachine.StateMachineRequest;
import com.hazelcast.jet.api.statemachine.InvalidEventException;
import com.hazelcast.jet.api.statemachine.StateMachineRequestProcessor;

public abstract class AbstractStateMachineImpl
        <Input extends StateMachineEvent,
                State extends StateMachineState,
                Output extends StateMachineOutput> implements StateMachine<Input, State, Output> {
    protected volatile Output output;

    protected volatile State state = defaultState();

    protected final String name;

    private final ILogger logger;

    private final Map<State, Map<Input, State>> stateTransitionMatrix;

    private final ApplicationContext applicationContext;

    private final StateMachineRequestProcessor<Input> processor;

    private final BlockingQueue<RequestPayLoad<Input, Output>> eventsQueue =
            new LinkedBlockingDeque<RequestPayLoad<Input, Output>>();

    protected AbstractStateMachineImpl(String name,
                                       Map<State, Map<Input, State>> stateTransitionMatrix,
                                       StateMachineRequestProcessor<Input> processor,
                                       NodeEngine nodeEngine,
                                       ApplicationContext applicationContext) {
        this.name = name;
        this.processor = processor;
        this.applicationContext = applicationContext;
        this.stateTransitionMatrix = stateTransitionMatrix;

        if (nodeEngine != null) {
            this.logger = nodeEngine.getLogger(getClass());
            getExecutor().addTask(new EventsProcessor(this.eventsQueue));
        } else {
            this.logger = null;
        }
    }

    protected abstract TaskExecutor getExecutor();

    protected abstract State defaultState();

    @Override
    public State currentState() {
        return state;
    }

    public <P> Future<Output> handleRequest(StateMachineRequest<Input, P> request) {
        RequestPayLoad<Input, Output> payLoad =
                new RequestPayLoad<Input, Output>(request.getContainerEvent(), request.getPayLoad());
        this.eventsQueue.offer(payLoad);
        return payLoad.getFuture();
    }

    protected abstract Output output(Input input, State nextState);

    @Override
    public Output getOutput() {
        return output;
    }

    private class EventsProcessor extends AbstractTask {
        private final Queue<RequestPayLoad<Input, Output>> requestsQueue;

        EventsProcessor(Queue<RequestPayLoad<Input, Output>> requestsQueue) {
            this.requestsQueue = requestsQueue;
        }

        @Override
        public boolean executeTask(Payload payload) {
            RequestPayLoad<Input, Output> requestHolder = this.requestsQueue.poll();

            if (requestHolder == null) {
                payload.set(false);
                return true;
            }

            Input event = requestHolder.getEvent();
            SettableFuture<Output> future = requestHolder.getFuture();

            try {
                Map<Input, State> transmissions = stateTransitionMatrix.get(state);

                if (transmissions == null) {
                    future.setException(new InvalidEventException(event, state, name));
                    return true;
                }

                State nextState = transmissions.get(event);

                if (nextState != null) {
                    if (processor != null) {
                        processor.processRequest(requestHolder.getEvent(), requestHolder.getPayLoad());
                    }

                    state = nextState;
                    output = output(event, nextState);
                    future.set(output);
                } else {
                    output = output(event, null);
                    Throwable error = new InvalidEventException(event, state, name);
                    logger.warning(error.getMessage(), error);
                    future.setException(error);
                }
            } catch (Throwable e) {
                if (logger != null) {
                    logger.warning(e.getMessage(), e);
                }

                future.setException(e);
            }

            return true;
        }
    }

    public ApplicationContext getApplicationContext() {
        return this.applicationContext;
    }
}
