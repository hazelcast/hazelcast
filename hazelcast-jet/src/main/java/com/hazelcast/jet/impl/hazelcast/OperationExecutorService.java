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

package com.hazelcast.jet.impl.hazelcast;


import java.util.Set;
import java.util.List;
import java.util.Arrays;

import com.hazelcast.jet.impl.util.JetUtil;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.ArrayList;

import org.slf4j.LoggerFactory;
import com.hazelcast.core.Member;


import java.util.concurrent.Future;

import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.spi.dag.DAG;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

import com.hazelcast.jet.impl.application.localization.ChunkIterator;
import com.hazelcast.jet.spi.CombinedJetException;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.InvocationFactory;

import com.hazelcast.jet.impl.application.LocalizationResource;
import com.hazelcast.jet.api.application.ApplicationStateManager;
import com.hazelcast.jet.api.application.ApplicationInvocationService;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;

public abstract class OperationExecutorService<PayLoad> implements ApplicationInvocationService<PayLoad> {
    protected static final Logger LOG = LoggerFactory.getLogger(OperationExecutorService.class);
    private final ExecutorService executorService;

    public OperationExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    protected OperationExecutor createInitiationApplicationExecutor(final JetApplicationConfig config,
                                                                    ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                null,
                ApplicationEvent.INIT_SUCCESS,
                ApplicationEvent.INIT_FAILURE,
                applicationStateManager,
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createInitApplicationInvoker(config);
                    }
                }
        );
    }

    protected OperationExecutor createFinalizationOperationExecutor(ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                ApplicationEvent.FINALIZATION_START,
                ApplicationEvent.FINALIZATION_SUCCESS,
                ApplicationEvent.FINALIZATION_FAILURE,
                applicationStateManager,
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createFinalizationInvoker();
                    }
                }
        );
    }


    protected OperationExecutor createInterruptionOperationExecutor(ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                ApplicationEvent.INTERRUPTION_START,
                ApplicationEvent.INTERRUPTION_SUCCESS,
                ApplicationEvent.INTERRUPTION_FAILURE,
                applicationStateManager,
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createInterruptInvoker();
                    }
                }
        );
    }

    protected OperationExecutor createExecutionOperationExecutor(ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                ApplicationEvent.EXECUTION_START,
                ApplicationEvent.EXECUTION_SUCCESS,
                ApplicationEvent.EXECUTION_FAILURE,
                applicationStateManager,
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createExecutionInvoker();
                    }
                });
    }

    public abstract PayLoad createSubmitInvoker(DAG dag);

    protected OperationExecutor createOperationSubmitExecutor(final DAG dag, ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                ApplicationEvent.SUBMIT_START,
                ApplicationEvent.SUBMIT_SUCCESS,
                ApplicationEvent.SUBMIT_FAILURE,
                applicationStateManager,
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createSubmitInvoker(dag);
                    }
                }
        );
    }

    protected OperationExecutor createLocalizationOperationExecutor(final Set<LocalizationResource> localizedResources,
                                                                    ApplicationStateManager applicationStateManager) {
        return new OperationExecutor(
                ApplicationEvent.LOCALIZATION_START,
                ApplicationEvent.LOCALIZATION_SUCCESS,
                ApplicationEvent.LOCALIZATION_FAILURE,
                applicationStateManager,
                new Runnable() {
                    @Override
                    public void run() {
                        executeApplicationLocalization(localizedResources);
                    }
                }
        );
    }

    protected List<Future> invokeInCluster(InvocationFactory<PayLoad> operationFactory) {
        Set<Member> members = getMembers();
        List<Future> futureList = new ArrayList<Future>(
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
        List<Throwable> errors = new ArrayList<Throwable>(
                list.size()
        );

        for (Future future : list) {
            try {
                future.get(getSecondsToAwait(), TimeUnit.SECONDS);
            } catch (Throwable e) {
                errors.add(e);
            }
        }

        if (errors.size() > 1) {
            throw new CombinedJetException(errors);
        } else if (errors.size() == 1) {
            reThrow(errors.get(0));
        }
    }


    private void publishEvent(final ApplicationEvent applicationEvent) {
        List<Future> futures = invokeInCluster(
                new InvocationFactory<PayLoad>() {
                    @Override
                    public PayLoad payLoad() {
                        return createEventInvoker(applicationEvent);
                    }
                }
        );

        await(futures);
    }

    protected void reThrow(Throwable e) {
        throw JetUtil.reThrow(e);
    }

    protected abstract JetApplicationConfig getConfig();

    protected int getSecondsToAwait() {
        return getConfig().getJetSecondsToAwait();
    }

    protected int getLocalizationChunkSize() {
        return getConfig().getChunkSize();
    }

    private void executeApplicationLocalization(final Set<LocalizationResource> localizedResources) {
        Iterator<Chunk> iterator = new ChunkIterator(
                localizedResources,
                getLocalizationChunkSize()
        );

        List<Future> futures = new ArrayList<Future>();

        while (iterator.hasNext()) {
            final Chunk chunk = iterator.next();

            InvocationFactory<PayLoad> operationFactory = new InvocationFactory<PayLoad>() {
                @Override
                public PayLoad payLoad() {
                    return createLocalizationInvoker(chunk);
                }
            };

            futures.addAll(invokeInCluster(
                    operationFactory
            ));
        }

        await(futures);

        InvocationFactory<PayLoad> operationFactory = new InvocationFactory<PayLoad>() {
            @Override
            public PayLoad payLoad() {
                return createAcceptedLocalizationInvoker();
            }
        };

        futures.addAll(
                invokeInCluster(
                        operationFactory
                ));

        await(futures);
    }

    final class OperationExecutor implements Runnable {
        private final Runnable executor;
        private final ApplicationEvent startEvent;
        private final ApplicationEvent successEvent;
        private final ApplicationEvent failureEvent;
        private final InvocationFactory<PayLoad> operationFactory;
        private final ApplicationStateManager applicationStateManager;

        public OperationExecutor(ApplicationEvent startEvent,
                                 ApplicationEvent successEvent,
                                 ApplicationEvent failureEvent,
                                 ApplicationStateManager applicationStateManager,
                                 InvocationFactory<PayLoad> invocationFactory
        ) {
            this.startEvent = startEvent;
            this.successEvent = successEvent;
            this.failureEvent = failureEvent;
            this.executor = null;
            this.operationFactory = invocationFactory;
            this.applicationStateManager = applicationStateManager;
        }

        public OperationExecutor(ApplicationEvent startEvent,
                                 ApplicationEvent successEvent,
                                 ApplicationEvent failureEvent,
                                 ApplicationStateManager applicationStateManager,
                                 Runnable operationExecutor
        ) {
            this.startEvent = startEvent;
            this.operationFactory = null;
            this.successEvent = successEvent;
            this.failureEvent = failureEvent;
            this.executor = operationExecutor;
            this.applicationStateManager = applicationStateManager;
        }

        private void executePayLoad() {
            if (this.startEvent != null) {
                publishEvent(this.startEvent);
                this.applicationStateManager.onEvent(this.startEvent);
            }

            try {
                if (this.executor == null) {
                    List<Future> futureList = invokeInCluster(
                            this.operationFactory
                    );

                    await(futureList);
                } else {
                    this.executor.run();
                }

                if (this.successEvent != null) {
                    if (this.successEvent != ApplicationEvent.FINALIZATION_SUCCESS) {
                        publishEvent(this.successEvent);
                    }

                    this.applicationStateManager.onEvent(this.successEvent);
                }
            } catch (Throwable e) {
                try {
                    if (this.failureEvent != null) {
                        publishEvent(this.failureEvent);
                        this.applicationStateManager.onEvent(this.failureEvent);
                    }
                } catch (Throwable ee) {
                    reThrow(new CombinedJetException(Arrays.asList(e, ee)));
                }

                reThrow(e);
            }
        }

        @Override
        public void run() {
            executePayLoad();
        }
    }
}
