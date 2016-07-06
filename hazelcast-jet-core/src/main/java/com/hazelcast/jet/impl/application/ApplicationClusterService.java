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

package com.hazelcast.jet.impl.application;


import com.hazelcast.core.Member;
import com.hazelcast.jet.CombinedJetException;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.impl.application.localization.ChunkIterator;
import com.hazelcast.jet.impl.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.statemachine.application.ApplicationStateMachine;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.JetUtil.reThrow;


public abstract class ApplicationClusterService<Payload> {
    protected final String name;
    protected ApplicationConfig applicationConfig;

    private final ExecutorService executorService;

    public ApplicationClusterService(String name,
                                     ExecutorService executorService) {
        this.name = name;
        this.executorService = executorService;
    }

    /**
     * Init application
     *
     * @param config                  application's config
     * @param applicationStateMachine
     */
    public void init(ApplicationConfig config,
                     ApplicationStateMachine applicationStateMachine) {
        applicationConfig = config;
        new OperationExecutor(
                null,
                ApplicationEvent.INIT_SUCCESS,
                ApplicationEvent.INIT_FAILURE,
                applicationStateMachine,
                () -> createInitApplicationInvoker(config)
        ).run();
    }

    /**
     * Performs localization operation
     *
     * @param localizedResources      classpath resources
     * @param applicationStateMachine manager to work with application state-machine
     */
    public void localize(Set<LocalizationResource> localizedResources,
                         ApplicationStateMachine applicationStateMachine) {
        new OperationExecutor(
                ApplicationEvent.LOCALIZATION_START,
                ApplicationEvent.LOCALIZATION_SUCCESS,
                ApplicationEvent.LOCALIZATION_FAILURE,
                applicationStateMachine,
                () -> executeApplicationLocalization(localizedResources)
        ).run();
    }

    /**
     * Execute application
     *
     * @param applicationStateMachine manager to work with application state-machine
     * @return awaiting Future
     */
    public Future execute(ApplicationStateMachine applicationStateMachine) {
        return this.executorService.submit(new OperationExecutor(
                ApplicationEvent.EXECUTION_START,
                ApplicationEvent.EXECUTION_SUCCESS,
                ApplicationEvent.EXECUTION_FAILURE,
                applicationStateMachine,
                this::createExecutionInvoker));
    }

    /**
     * Interrupt application
     *
     * @param applicationStateMachine manager to work with application state-machine
     * @return awaiting Future
     */
    public Future interrupt(ApplicationStateMachine applicationStateMachine) {
        return this.executorService.submit(new OperationExecutor(
                ApplicationEvent.INTERRUPTION_START,
                ApplicationEvent.INTERRUPTION_SUCCESS,
                ApplicationEvent.INTERRUPTION_FAILURE,
                applicationStateMachine,
                this::createInterruptInvoker
        ));
    }

    /**
     * Finalize application
     *
     * @param applicationStateMachine manager to work with application state-machine
     * @return awaiting Future
     */
    public Future destroy(ApplicationStateMachine applicationStateMachine) {
        return this.executorService.submit(new OperationExecutor(
                ApplicationEvent.FINALIZATION_START,
                ApplicationEvent.FINALIZATION_SUCCESS,
                ApplicationEvent.FINALIZATION_FAILURE,
                applicationStateMachine,
                () -> {
                }));
    }

    /**
     * Submits dag for the application
     *
     * @param dag                     direct acyclic graph
     * @param applicationStateMachine manager to work with application state-machine
     */
    public void submitDag(DAG dag, ApplicationStateMachine applicationStateMachine) {
        new OperationExecutor(
                ApplicationEvent.SUBMIT_START,
                ApplicationEvent.SUBMIT_SUCCESS,
                ApplicationEvent.SUBMIT_FAILURE,
                applicationStateMachine,
                () -> createSubmitInvoker(dag)
        ).run();
    }

    /**
     * @return accumulators
     */
    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> getAccumulators() {
        Set<Member> members = this.getMembers();
        Map<String, Accumulator> cache = new HashMap<>();

        try {
            for (Member member : members) {
                Callable callable =
                        createInvocation(
                                member,
                                this::createAccumulatorsInvoker
                        );

                Map<String, Accumulator> memberResponse = readAccumulatorsResponse(callable);

                for (Map.Entry<String, Accumulator> entry : memberResponse.entrySet()) {
                    String key = entry.getKey();
                    Accumulator accumulator = entry.getValue();

                    Accumulator collector = cache.get(key);

                    if (collector == null) {
                        cache.put(key, accumulator);
                    } else {
                        collector.merge(accumulator);
                    }
                }
            }
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }

        return Collections.unmodifiableMap(cache);
    }

    /**
     * @return member of JET cluster
     */
    protected abstract Set<Member> getMembers();

    /**
     * @return invoker for interrupt operation
     */
    protected abstract Payload createInterruptInvoker();

    /**
     * @return invoker for execute operation
     */
    protected abstract Payload createExecutionInvoker();

    /**
     * @return invoker to work with accumulators
     */
    protected abstract Payload createAccumulatorsInvoker();

    /**
     * @return invoker to accept application's localization
     */
    protected abstract Payload createAcceptedLocalizationInvoker();

    /**
     * @param chunk chunk of byte-code
     * @return invoker to localize application
     */
    protected abstract Payload createLocalizationInvoker(Chunk chunk);

    /**
     * Invoker to send JET event
     *
     * @param applicationEvent JET event
     * @return invoker
     */
    protected abstract Payload createEventInvoker(ApplicationEvent applicationEvent);

    /**
     * Return invoker to init JET application
     *
     * @param config application config
     * @return invoker to init application
     */
    protected abstract Payload createInitApplicationInvoker(ApplicationConfig config);

    /**
     * Creates invocation to be called on the corresponding member
     *
     * @param member           member where invocation should be executed
     * @param operationFactory factory for operations
     * @param <T>              type of the return value
     * @return Callable object for the corresponding invocation
     */
    protected abstract <T> Callable<T> createInvocation(Member member,
                                                        Supplier<Payload> operationFactory);

    protected abstract Payload createSubmitInvoker(DAG dag);

    protected abstract <T> T toObject(com.hazelcast.nio.serialization.Data data);

    protected abstract Map<String, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception;

    protected abstract ApplicationConfig getApplicationConfig();

    protected int getSecondsToAwait() {
        return getApplicationConfig().getSecondsToAwait();
    }

    protected int getLocalizationChunkSize() {
        return getApplicationConfig().getChunkSize();
    }

    protected List<Future> invokeInCluster(Supplier<Payload> operationFactory) {
        Set<Member> members = getMembers();
        List<Future> futureList = new ArrayList<>(
                members.size()
        );

        for (Member member : members) {
            futureList.add(
                    this.executorService.submit(
                            createInvocation(member, operationFactory)
                    ));
        }

        return futureList;
    }

    private void await(List<Future> list) {
        List<Throwable> errors = new ArrayList<>(
                list.size()
        );

        for (Future future : list) {
            try {
                future.get();
            } catch (ExecutionException e) {
                errors.add(e.getCause());
            } catch (InterruptedException e) {
                errors.add(e);
            }
        }

        if (errors.size() > 1) {
            throw new CombinedJetException(errors);
        }
        if (errors.size() == 1) {
            throw reThrow(errors.get(0));
        }
    }

    private void publishEvent(final ApplicationEvent applicationEvent) {
        List<Future> futures = invokeInCluster(
                () -> createEventInvoker(applicationEvent)
        );

        await(futures);
    }

    private void executeApplicationLocalization(final Set<LocalizationResource> localizedResources) {
        Iterator<Chunk> iterator = new ChunkIterator(
                localizedResources,
                getLocalizationChunkSize()
        );

        List<Future> futures = new ArrayList<>();

        while (iterator.hasNext()) {
            final Chunk chunk = iterator.next();
            Supplier<Payload> operationFactory = () -> createLocalizationInvoker(chunk);
            futures.addAll(invokeInCluster(operationFactory));
        }

        await(futures);
        Supplier<Payload> operationFactory = this::createAcceptedLocalizationInvoker;
        futures.addAll(invokeInCluster(operationFactory));
        await(futures);
    }

    final class OperationExecutor implements Runnable {
        private final Runnable executor;
        private final ApplicationEvent startEvent;
        private final ApplicationEvent successEvent;
        private final ApplicationEvent failureEvent;
        private final Supplier<Payload> invocationFactory;
        private final ApplicationStateMachine applicationStateMachine;

        public OperationExecutor(ApplicationEvent startEvent,
                                 ApplicationEvent successEvent,
                                 ApplicationEvent failureEvent,
                                 ApplicationStateMachine applicationStateMachine,
                                 Supplier<Payload> invocationFactory
        ) {
            this.startEvent = startEvent;
            this.successEvent = successEvent;
            this.failureEvent = failureEvent;
            this.executor = null;
            this.invocationFactory = invocationFactory;
            this.applicationStateMachine = applicationStateMachine;
        }

        public OperationExecutor(ApplicationEvent startEvent,
                                 ApplicationEvent successEvent,
                                 ApplicationEvent failureEvent,
                                 ApplicationStateMachine applicationStateMachine,
                                 Runnable operationExecutor
        ) {
            this.startEvent = startEvent;
            this.invocationFactory = null;
            this.successEvent = successEvent;
            this.failureEvent = failureEvent;
            this.executor = operationExecutor;
            this.applicationStateMachine = applicationStateMachine;
        }

        @Override
        public void run() {
            if (this.startEvent != null) {
                publishEvent(this.startEvent);
                this.applicationStateMachine.onEvent(this.startEvent);
            }

            try {
                if (this.executor == null) {
                    List<Future> futureList = invokeInCluster(
                            this.invocationFactory
                    );

                    await(futureList);
                } else {
                    this.executor.run();
                }

                if (this.successEvent != null) {
                    if (this.successEvent != ApplicationEvent.FINALIZATION_SUCCESS) {
                        publishEvent(this.successEvent);
                    }

                    this.applicationStateMachine.onEvent(this.successEvent);
                }
            } catch (Throwable e) {
                try {
                    if (this.failureEvent != null) {
                        publishEvent(this.failureEvent);
                        this.applicationStateMachine.onEvent(this.failureEvent);
                    }
                } catch (Throwable ee) {
                    throw reThrow(new CombinedJetException(Arrays.asList(e, ee)));
                }

                throw reThrow(e);
            }
        }
    }
}
