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

package com.hazelcast.jet.internal.impl.application;


import com.hazelcast.core.Member;
import com.hazelcast.jet.internal.api.hazelcast.InvocationFactory;
import com.hazelcast.jet.internal.api.hazelcast.JetService;
import com.hazelcast.jet.internal.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.internal.impl.application.localization.Chunk;
import com.hazelcast.jet.internal.impl.hazelcast.AbstractApplicationClusterService;
import com.hazelcast.jet.internal.impl.operation.application.AcceptLocalizationOperation;
import com.hazelcast.jet.internal.impl.operation.application.ApplicationEventOperation;
import com.hazelcast.jet.internal.impl.operation.application.ExecutionApplicationRequestOperation;
import com.hazelcast.jet.internal.impl.operation.application.FinalizationApplicationRequestOperation;
import com.hazelcast.jet.internal.impl.operation.application.GetAccumulatorsOperation;
import com.hazelcast.jet.internal.impl.operation.application.InitApplicationRequestOperation;
import com.hazelcast.jet.internal.impl.operation.application.InterruptExecutionOperation;
import com.hazelcast.jet.internal.impl.operation.application.LocalizationChunkOperation;
import com.hazelcast.jet.internal.impl.operation.application.SubmitApplicationRequestOperation;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.jet.api.container.CounterKey;
import com.hazelcast.jet.api.counters.Accumulator;
import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;


public class ServerApplicationClusterService extends AbstractApplicationClusterService<Operation> {
    private final NodeEngine nodeEngine;
    private final JetService jetService;

    public ServerApplicationClusterService(String name,
                                           ExecutorService executorService,
                                           NodeEngine nodeEngine,
                                           JetService jetService) {
        super(name, executorService);

        this.nodeEngine = nodeEngine;
        this.jetService = jetService;
    }

    public Operation createInitApplicationInvoker(JetApplicationConfig config) {
        return new InitApplicationRequestOperation(
                this.name,
                config
        );
    }

    @Override
    public Operation createFinalizationInvoker() {
        return new FinalizationApplicationRequestOperation(
                this.name
        );
    }

    @Override
    public Operation createInterruptInvoker() {
        return new InterruptExecutionOperation(
                this.name
        );
    }

    @Override
    public Operation createExecutionInvoker() {
        return new ExecutionApplicationRequestOperation(
                this.name
        );
    }

    @Override
    public Operation createAccumulatorsInvoker() {
        return new GetAccumulatorsOperation(
                this.name
        );
    }

    @Override
    public Operation createSubmitInvoker(DAG dag) {
        return new SubmitApplicationRequestOperation(
                this.name,
                dag
        );
    }

    @Override
    public Operation createLocalizationInvoker(Chunk chunk) {
        return new LocalizationChunkOperation(this.name, chunk);
    }

    @Override
    public Operation createAcceptedLocalizationInvoker() {
        return new AcceptLocalizationOperation(this.name);
    }

    public Operation createEventInvoker(ApplicationEvent applicationEvent) {
        return new ApplicationEventOperation(
                applicationEvent,
                this.name
        );
    }

    @Override
    public Set<Member> getMembers() {
        return this.nodeEngine.getClusterService().getMembers();
    }

    @Override
    protected JetApplicationConfig getJetApplicationConfig() {
        return JetUtil.resolveJetServerApplicationConfig(this.nodeEngine, this.jetApplicationConfig, name);
    }

    @Override
    public <T> Callable<T> createInvocation(Member member,
                                            InvocationFactory<Operation> factory) {
        return new ServerApplicationInvocation<T>(
                factory.payLoad(),
                member.getAddress(),
                this.jetService,
                this.nodeEngine
        );
    }

    @Override
    protected <T> T toObject(Data data) {
        return this.nodeEngine.toObject(data);
    }

    @SuppressWarnings("unchecked")
    public Map<CounterKey, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception {
        return (Map<CounterKey, Accumulator>) callable.call();
    }
}
