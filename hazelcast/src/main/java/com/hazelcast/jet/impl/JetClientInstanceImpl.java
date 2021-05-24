/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl;

import com.hazelcast.client.impl.client.DistributedObjectInfo;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.BasicJob;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.client.protocol.codec.JetExistsDistributedObjectCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobSummaryListCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation.GetJobIdsResult;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static com.hazelcast.jet.impl.operation.GetJobIdsOperation.ALL_JOBS;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final SerializationService serializationService;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.serializationService = client.getSerializationService();

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Nonnull @Override
    public BasicJob newLightJobInt(Object jobDefinition) {
        Data jobDefinitionSerialized = serializationService.toData(jobDefinition);
        long jobId = newJobId();
        ClientMessage message = JetSubmitJobCodec.encodeRequest(jobId, jobDefinitionSerialized, null, true);

        // find random non-lite member
        Member[] members = client.getCluster().getMembers().toArray(new Member[0]);
        int randomMemberIndex = ThreadLocalRandom.current().nextInt(members.length);
        for (int i = 0; i < members.length && members[randomMemberIndex].isLiteMember(); i++) {
            randomMemberIndex++;
            if (randomMemberIndex == members.length) {
                randomMemberIndex = 0;
            }
        }
        UUID coordinatorUuid = members[randomMemberIndex].getUuid();
        ClientInvocation invocation = new ClientInvocation(client, message, null, coordinatorUuid);

        ClientInvocationFuture future = invocation.invoke();
        return new ClientLightJobProxy(this, coordinatorUuid, jobId, future);
    }

    @Nonnull @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Nonnull @Override
    public List<BasicJob> getAllJobs() {
        GetJobIdsResult response = getJobIdsInternal(null, ALL_JOBS);
        List<BasicJob> result = new ArrayList<>(response.getJobIds().length);
        long[] jobIds = response.getJobIds();
        for (int i = 0; i < jobIds.length; i++) {
            long jobId = response.getJobIds()[i];
            UUID uuid = response.getCoordinators()[i];
            result.add(uuid != null
                    ? new ClientLightJobProxy(this, uuid, jobId, null)
                    : new ClientJobProxy(this, jobId));
        }

        return result;
    }

    private GetJobIdsResult getJobIdsInternal(@Nullable String onlyName, long onlyJobId) {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobIdsCodec.encodeRequest(onlyName, onlyJobId), resp -> {
            Data responseSerialized = JetGetJobIdsCodec.decodeResponse(resp).response;
            return serializationService.toObject(responseSerialized);
        });
    }

    /**
     * Returns a list of jobs and a summary of their details.
     */
    @Nonnull
    public List<JobSummary> getJobSummaryList() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobSummaryListCodec.encodeRequest(),
                response -> JetGetJobSummaryListCodec.decodeResponse(response));
    }

    @Nonnull
    public HazelcastClientInstanceImpl getHazelcastClient() {
        return client;
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetExistsDistributedObjectCodec.encodeRequest(serviceName, objectName),
                response -> JetExistsDistributedObjectCodec.decodeResponse(response)
        );
    }

    public List<DistributedObjectInfo> getDistributedObjects() {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                ClientGetDistributedObjectsCodec.encodeRequest(),
                response -> ClientGetDistributedObjectsCodec.decodeResponse(response)
        );
    }

    @Override
    public List<Long> getJobIdsByName(String name) {
        long[] jobIds = getJobIdsInternal(name, ALL_JOBS).getJobIds();
        return new AbstractList<Long>() {
            @Override
            public Long get(int index) {
                return jobIds[index];
            }

            @Override
            public int size() {
                return jobIds.length;
            }
        };
    }

    @Override
    public Job newJobProxy(long jobId, Object jobDefinition, JobConfig config) {
        return new ClientJobProxy(this, jobId, jobDefinition, config);
    }

    @Override
    public Job newJobProxy(long jobId) {
        return new ClientJobProxy(this, jobId);
    }

    @Override
    public ILogger getLogger() {
        return client.getLoggingService().getLogger(getClass());
    }

    private <S> S invokeRequestOnMasterAndDecodeResponse(ClientMessage request,
                                                         Function<ClientMessage, Object> decoder) {
        UUID masterUuid = client.getClientClusterService().getMasterMember().getUuid();
        return invokeRequestAndDecodeResponse(masterUuid, request, decoder);
    }

    private <S> S invokeRequestOnAnyMemberAndDecodeResponse(ClientMessage request,
                                                            Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(null, request, decoder);
    }

    private <S> S invokeRequestAndDecodeResponse(UUID uuid, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, uuid);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }
}
