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

package com.hazelcast.jet.impl.hazelcast.client;


import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetAcceptLocalizationCodec;
import com.hazelcast.client.impl.protocol.codec.JetEventCodec;
import com.hazelcast.client.impl.protocol.codec.JetExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.JetFinalizeApplicationCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetAccumulatorsCodec;
import com.hazelcast.client.impl.protocol.codec.JetInitCodec;
import com.hazelcast.client.impl.protocol.codec.JetInterruptCodec;
import com.hazelcast.client.impl.protocol.codec.JetLocalizeCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitCodec;
import com.hazelcast.core.Member;
import com.hazelcast.jet.api.hazelcast.InvocationFactory;
import com.hazelcast.jet.api.statemachine.application.ApplicationEvent;
import com.hazelcast.jet.impl.application.localization.Chunk;
import com.hazelcast.jet.impl.hazelcast.AbstractApplicationClusterService;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.config.JetApplicationConfig;
import com.hazelcast.jet.spi.container.CounterKey;
import com.hazelcast.jet.spi.counters.Accumulator;
import com.hazelcast.jet.spi.dag.DAG;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;


public class ClientApplicationClusterService
        extends AbstractApplicationClusterService<ClientMessage> {
    private final HazelcastClientInstanceImpl client;

    public ClientApplicationClusterService(
            HazelcastClientInstanceImpl instance,
            String name,
            ExecutorService executorService) {
        super(name, executorService);
        this.client = instance;
    }

    @Override
    public ClientMessage createInitApplicationInvoker(JetApplicationConfig config) {
        return JetInitCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(
                        config == null ? new JetApplicationConfig() : config
                )
        );
    }

    @Override
    public ClientMessage createInterruptInvoker() {
        return JetInterruptCodec.encodeRequest(this.name);
    }

    @Override
    public ClientMessage createExecutionInvoker() {
        return JetExecuteCodec.encodeRequest(this.name);
    }

    @Override
    public ClientMessage createAccumulatorsInvoker() {
        return JetGetAccumulatorsCodec.encodeRequest(this.name);
    }

    @Override
    public ClientMessage createSubmitInvoker(DAG dag) {
        return JetSubmitCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(dag)
        );
    }

    @Override
    public ClientMessage createLocalizationInvoker(Chunk chunk) {
        return JetLocalizeCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(chunk)
        );
    }

    @Override
    public ClientMessage createAcceptedLocalizationInvoker() {
        return JetAcceptLocalizationCodec.encodeRequest(
                this.name
        );
    }

    @Override
    public ClientMessage createFinalizationInvoker() {
        return JetFinalizeApplicationCodec.encodeRequest(this.name);
    }

    @Override
    public ClientMessage createEventInvoker(ApplicationEvent applicationEvent) {
        return JetEventCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(applicationEvent)
        );
    }

    @Override
    public Set<Member> getMembers() {
        return this.client.getCluster().getMembers();
    }

    @Override
    protected JetApplicationConfig getJetApplicationConfig() {
        return JetUtil.resolveJetClientApplicationConfig(this.client, this.jetApplicationConfig, name);
    }

    @Override
    public <T> Callable<T> createInvocation(Member member,
                                            InvocationFactory<ClientMessage> factory) {
        return new ClientApplicationInvocation<T>(
                factory.payLoad(),
                member.getAddress(),
                this.client
        );
    }

    @Override
    protected <T> T toObject(Data data) {
        return this.client.getSerializationService().toObject(data);
    }

    @Override
    public Map<CounterKey, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception {
        ClientMessage clientMessage = (ClientMessage) callable.call();
        JetGetAccumulatorsCodec.ResponseParameters responseParameters =
                JetGetAccumulatorsCodec.decodeResponse(clientMessage);

        return toObject(responseParameters.response);
    }
}
