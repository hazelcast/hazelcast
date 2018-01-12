/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsByNameCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.Cluster;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.stream.Collectors.toList;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private SerializationService serializationService;

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
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = uploadResourcesAndAssignId(config);
        return new ClientJobProxy(client, jobId, dag, config);
    }

    @Nonnull @Override
    public List<Job> getJobs() {
        ClientInvocation invocation = new ClientInvocation(
                client, JetGetJobIdsCodec.encodeRequest(), null, masterAddress(client.getCluster())
        );
        return uncheckCall(() -> {
            ClientMessage response = invocation.invoke().get();
            Set<Long> jobs = serializationService.toObject(JetGetJobIdsCodec.decodeResponse(response).response);
            return jobs.stream().map(jobId -> new ClientJobProxy(client, jobId)).collect(toList());
        });
    }

    @Override
    public Job getJob(long jobId) {
        try {
            Job job = new ClientJobProxy(client, jobId);
            job.getStatus();
            return job;
        } catch (Exception e) {
            if (peel(e) instanceof JobNotFoundException) {
                return null;
            }

            throw e;
        }
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return getJobIdsByName(name).stream().map(jobId -> new ClientJobProxy(client, jobId)).collect(toList());
    }

    private List<Long> getJobIdsByName(String name) {
        ClientInvocation invocation = new ClientInvocation(
                client, JetGetJobIdsByNameCodec.encodeRequest(name), null, masterAddress(client.getCluster())
        );

        return uncheckCall(() -> {
            ClientMessage response = invocation.invoke().get();
            List<Long> jobs = serializationService.toObject(JetGetJobIdsByNameCodec.decodeResponse(response).response);
            return jobs;
        });
    }

    private static Address masterAddress(Cluster cluster) {
        return cluster.getMembers().stream().findFirst()
                      .orElseThrow(() -> new IllegalStateException("No members found in cluster"))
                      .getAddress();
    }
}
