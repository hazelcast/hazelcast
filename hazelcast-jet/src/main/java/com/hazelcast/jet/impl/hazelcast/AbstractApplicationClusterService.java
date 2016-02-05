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
import java.util.Map;
import java.util.HashMap;


import com.hazelcast.core.Member;

import java.util.concurrent.Future;

import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.dag.DAG;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;

import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.api.hazelcast.InvocationFactory;
import com.hazelcast.jet.impl.application.LocalizationResource;
import com.hazelcast.jet.api.application.ApplicationStateManager;
import com.hazelcast.jet.api.application.ApplicationClusterService;
import com.hazelcast.jet.impl.util.JetUtil;


public abstract class AbstractApplicationClusterService<PayLoad>
        extends OperationExecutorService<PayLoad>
        implements ApplicationClusterService {
    protected final String name;
    protected final ExecutorService executorService;

    public AbstractApplicationClusterService(String name,
                                             ExecutorService executorService) {
        super(executorService);
        this.name = name;
        this.executorService = executorService;
    }

    @Override
    public void initApplication(JetApplicationConfig config,
                                ApplicationStateManager applicationStateManager) {
        createInitiationApplicationExecutor(config, applicationStateManager).run();
    }

    @Override
    public void localizeApplication(Set<LocalizationResource> localizedResources,
                                    ApplicationStateManager applicationStateManager) {
        createLocalizationOperationExecutor(localizedResources, applicationStateManager).run();
    }

    @Override
    public Future executeApplication(ApplicationStateManager applicationStateManager) {
        return this.executorService.submit(createExecutionOperationExecutor(applicationStateManager));
    }

    @Override
    public Future interruptApplication(ApplicationStateManager applicationStateManager) {
        return this.executorService.submit(createInterruptionOperationExecutor(applicationStateManager));
    }

    @Override
    public Future finalizeApplication(ApplicationStateManager applicationStateManager) {
        return this.executorService.submit(createFinalizationOperationExecutor(applicationStateManager));
    }

    @Override
    public void submitDag(DAG dag, ApplicationStateManager applicationStateManager) {
        createOperationSubmitExecutor(dag, applicationStateManager).run();
    }

    protected abstract <T> T toObject(com.hazelcast.nio.serialization.Data data);

    protected abstract Map<CounterKey, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception;

    @SuppressWarnings("unchecked")
    public Map<CounterKey, Accumulator> getAccumulators() {
        Set<Member> members = this.getMembers();
        Map<CounterKey, Accumulator> cache = new HashMap<CounterKey, Accumulator>();

        try {
            for (Member member : members) {
                Callable callable =
                        createInvocation(
                                member,
                                new InvocationFactory<PayLoad>() {
                                    @Override
                                    public PayLoad payLoad() {
                                        return createAccumulatorsInvoker();
                                    }
                                }
                        );

                Map<CounterKey, Accumulator> memberResponse = readAccumulatorsResponse(callable);

                for (Map.Entry<CounterKey, Accumulator> entry : memberResponse.entrySet()) {
                    CounterKey counterKey = entry.getKey();
                    Accumulator accumulator = entry.getValue();

                    Accumulator collector = cache.get(counterKey);

                    if (collector == null) {
                        cache.put(counterKey, accumulator);
                    } else {
                        collector.merge(accumulator);
                    }
                }
            }
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }

        return cache;
    }
}
