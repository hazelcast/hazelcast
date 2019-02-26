/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetDistributedObjectsCodec;
import com.hazelcast.client.impl.protocol.codec.JetExistsDistributedObjectCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetClusterMetadataCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetClusterMetadataCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsByNameCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSummaryListCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetMemberXmlConfigurationCodec;
import com.hazelcast.client.impl.protocol.codec.JetReadMetricsCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryXmlConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.metrics.management.ConcurrentArrayRingbuffer.RingbufferSlice;
import com.hazelcast.jet.impl.metrics.management.MetricsResultSet;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final SerializationService serializationService;

    private final ClientMessageDecoder decodeMetricsResponse = new ClientMessageDecoder() {
        @Override
        public <T> T decodeClientMessage(ClientMessage msg) {
            RingbufferSlice<Map.Entry<Long, byte[]>> deserialized =
                    serializationService.toObject(JetReadMetricsCodec.decodeResponse(msg).response);
            return (T) new MetricsResultSet(deserialized);
        }
    };

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.serializationService = client.getSerializationService();

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Nonnull @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Nonnull @Override
    public List<Job> getJobs() {
        ClientInvocation invocation = new ClientInvocation(
                client, JetGetJobIdsCodec.encodeRequest(), null, masterAddress(client.getCluster())
        );

        try {
            ClientMessage response = invocation.invoke().get();
            List<Long> jobs = JetGetJobIdsCodec.decodeResponse(response).response;
            return jobs.stream().map(jobId -> new ClientJobProxy(this, jobId)).collect(toList());
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    /**
     * Reads the metrics journal for a given number starting from a specific sequence.
     */
    @Nonnull
    public ICompletableFuture<MetricsResultSet> readMetricsAsync(Member member, long startSequence) {
        ClientMessage request = JetReadMetricsCodec.encodeRequest(member.getUuid(), startSequence);
        ClientInvocation invocation = new ClientInvocation(client, request, null, member.getAddress());
        return new ClientDelegatingFuture<>(
                invocation.invoke(), serializationService, decodeMetricsResponse, false
        );
    }

    /**
     * Returns a list of jobs and a summary of their details.
     */
    @Nonnull
    public List<JobSummary> getJobSummaryList() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobSummaryListCodec.encodeRequest(),
                response -> JetGetJobSummaryListCodec.decodeResponse(response).response);
    }

    /**
     * Returns summary of cluster details.
     */
    @Nonnull
    public ClusterMetadata getClusterMetadata() {
        return invokeRequestOnMasterAndDecodeResponse(JetGetClusterMetadataCodec.encodeRequest(),
                response -> {
                    ResponseParameters parameters = JetGetClusterMetadataCodec.decodeResponse(response);
                    ClusterMetadata metadata = new ClusterMetadata();
                    metadata.setClusterTime(parameters.clusterTime);
                    metadata.setName(parameters.name);
                    metadata.setState(parameters.state);
                    metadata.setVersion(parameters.version);
                    return metadata;
                });
    }

    /**
     * Returns the member configuration.
     */
    @Nonnull
    public Config getHazelcastConfig() {
        String configString = invokeRequestOnMasterAndDecodeResponse(JetGetMemberXmlConfigurationCodec.encodeRequest(),
                response -> JetGetMemberXmlConfigurationCodec.decodeResponse(response).response);
        return new InMemoryXmlConfig(configString);
    }

    @Nonnull
    public HazelcastClientInstanceImpl getHazelcastClient() {
        return client;
    }

    @Override
    public boolean existsDistributedObject(@Nonnull String serviceName, @Nonnull String objectName) {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                JetExistsDistributedObjectCodec.encodeRequest(serviceName, objectName),
                response -> JetExistsDistributedObjectCodec.decodeResponse(response).response
        );
    }

    public List<DistributedObjectInfo> getDistributedObjects() {
        return invokeRequestOnAnyMemberAndDecodeResponse(
                ClientGetDistributedObjectsCodec.encodeRequest(),
                response -> ClientGetDistributedObjectsCodec.decodeResponse(response).response
        );
    }

    @Override
    public List<Long> getJobIdsByName(String name) {
        return invokeRequestOnMasterAndDecodeResponse(JetGetJobIdsByNameCodec.encodeRequest(name),
                response -> JetGetJobIdsByNameCodec.decodeResponse(response).response);
    }

    @Override
    public Job newJobProxy(long jobId, DAG dag, JobConfig config) {
        return new ClientJobProxy(this, jobId, dag, config);
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
        return invokeRequestAndDecodeResponse(masterAddress(client.getCluster()), request, decoder);
    }

    private <S> S invokeRequestOnAnyMemberAndDecodeResponse(ClientMessage request,
                                                            Function<ClientMessage, Object> decoder) {
        return invokeRequestAndDecodeResponse(null, request, decoder);
    }

    private <S> S invokeRequestAndDecodeResponse(Address address, ClientMessage request,
                                                 Function<ClientMessage, Object> decoder) {
        ClientInvocation invocation = new ClientInvocation(client, request, null, address);
        try {
            ClientMessage response = invocation.invoke().get();
            return serializationService.toObject(decoder.apply(response));
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private static Address masterAddress(Cluster cluster) {
        return cluster.getMembers().stream().findFirst()
                      .orElseThrow(() -> new IllegalStateException("No members found in cluster"))
                      .getAddress();
    }
}
