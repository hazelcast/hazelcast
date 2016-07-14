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

package com.hazelcast.jet.impl.job.client;


import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetAcceptLocalizationCodec;
import com.hazelcast.client.impl.protocol.codec.JetEventCodec;
import com.hazelcast.client.impl.protocol.codec.JetExecuteCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetAccumulatorsCodec;
import com.hazelcast.client.impl.protocol.codec.JetInitCodec;
import com.hazelcast.client.impl.protocol.codec.JetInterruptCodec;
import com.hazelcast.client.impl.protocol.codec.JetLocalizeCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitCodec;
import com.hazelcast.core.Member;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.JobClusterService;
import com.hazelcast.jet.impl.job.localization.Chunk;
import com.hazelcast.jet.impl.statemachine.job.JobEvent;
import com.hazelcast.nio.serialization.Data;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;


public class ClientJobClusterService
        extends JobClusterService<ClientMessage> {
    private final HazelcastClientInstanceImpl client;

    public ClientJobClusterService(
            HazelcastClientInstanceImpl instance,
            String name,
            ExecutorService executorService) {
        super(name, executorService);
        this.client = instance;
    }

    @Override
    public ClientMessage createInitJobInvoker(JobConfig config) {
        return JetInitCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(
                        config == null ? new JobConfig() : config
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
    public ClientMessage createEventInvoker(JobEvent jobEvent) {
        return JetEventCodec.encodeRequest(
                this.name,
                this.client.getSerializationService().toData(jobEvent)
        );
    }

    @Override
    public Set<Member> getMembers() {
        return client.getCluster().getMembers();
    }

    @Override
    protected JobConfig getJobConfig() {
        return jobConfig;
    }

    @Override
    protected <T> Callable<T> createInvocation(Member member,
                                               Supplier<ClientMessage> factory) {
        return new ClientJobInvocation<T>(
                factory.get(),
                member.getAddress(),
                this.client
        );
    }

    @Override
    protected <T> T toObject(Data data) {
        return this.client.getSerializationService().toObject(data);
    }

    @Override
    public Map<String, Accumulator> readAccumulatorsResponse(Callable callable) throws Exception {
        ClientMessage clientMessage = (ClientMessage) callable.call();
        JetGetAccumulatorsCodec.ResponseParameters responseParameters =
                JetGetAccumulatorsCodec.decodeResponse(clientMessage);

        return toObject(responseParameters.response);
    }
}
