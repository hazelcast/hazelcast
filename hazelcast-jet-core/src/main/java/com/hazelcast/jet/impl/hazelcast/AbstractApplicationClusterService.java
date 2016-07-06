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


import com.hazelcast.core.Member;
import com.hazelcast.jet.config.ApplicationConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.application.ApplicationClusterService;
import com.hazelcast.jet.impl.application.ApplicationStateManager;
import com.hazelcast.jet.impl.application.LocalizationResource;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


public abstract class AbstractApplicationClusterService<Payload>
        extends OperationExecutorService<Payload>
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
    public void initApplication(ApplicationConfig config,
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

    protected abstract Map<String, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception;

    @SuppressWarnings("unchecked")
    public Map<String, Accumulator> getAccumulators() {
        Set<Member> members = this.getMembers();
        Map<String, Accumulator> cache = new HashMap<String, Accumulator>();

        try {
            for (Member member : members) {
                Callable callable =
                        createInvocation(
                                member,
                                new InvocationFactory<Payload>() {
                                    @Override
                                    public Payload payload() {
                                        return createAccumulatorsInvoker();
                                    }
                                }
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
}
