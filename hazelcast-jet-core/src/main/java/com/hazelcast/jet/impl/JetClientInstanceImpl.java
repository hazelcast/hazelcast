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
import com.hazelcast.client.impl.protocol.codec.JetCancelJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobIdsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.idToString;
import static java.util.stream.Collectors.toList;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private SerializationService serializationService;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.logger = getLogger(JetInstance.class);
        this.serializationService = client.getSerializationService();

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Override
    public Job newJob(DAG dag) {
        SubmittedJobImpl job = new SubmittedJobImpl(this, getLogger(SubmittedJobImpl.class), dag, new JobConfig());
        job.init();
        return job;
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        SubmittedJobImpl job = new SubmittedJobImpl(this, getLogger(SubmittedJobImpl.class), dag, config);
        job.init();
        return job;
    }

    @Override
    public Collection<Job> getJobs() {
        ClientMessage request = JetGetJobIdsCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(client, request, null, masterAddress());
        Set<Long> jobIds;
        try {
            ClientMessage clientMessage = invocation.invoke().get();
            JetGetJobIdsCodec.ResponseParameters response = JetGetJobIdsCodec.decodeResponse(clientMessage);
            jobIds = serializationService.toObject(response.response);
        } catch (Exception e) {
            throw rethrow(e);
        }

        List<Job> jobs = jobIds.stream().map(jobId ->
                new TrackedJobImpl(getLogger(TrackedJobImpl.class), jobId))
                               .collect(toList());

        jobs.forEach(job -> ((TrackedJobImpl) job).init());

        return jobs;
    }

    private JobStatus sendJobStatusRequest(long jobId, boolean retryOnNotFound) {
        ClientMessage request = JetGetJobStatusCodec.encodeRequest(jobId, retryOnNotFound);
        ClientInvocation invocation = new ClientInvocation(client, request, jobObjectName(jobId), masterAddress());
        try {
            ClientMessage clientMessage = invocation.invoke().get();
            JetGetJobStatusCodec.ResponseParameters response = JetGetJobStatusCodec.decodeResponse(clientMessage);
            return serializationService.toObject(response.response);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    private ILogger getLogger(Class type) {
        return client.getLoggingService().getLogger(type);
    }

    private Address masterAddress() {
        Optional<Member> first = client.getCluster().getMembers().stream().findFirst();
        return first.orElseThrow(() -> new IllegalStateException("No members found in cluster")).getAddress();
    }

    private static String jobObjectName(long jobId) {
        return "jobId=" + idToString(jobId);
    }

    private class SubmittedJobImpl extends AbstractSubmittedJobImpl {

        SubmittedJobImpl(JetInstance jetInstance, ILogger logger, DAG dag, JobConfig config) {
            super(jetInstance, logger, dag, config);
        }

        @Override
        protected Address getMasterAddress() {
            return JetClientInstanceImpl.this.masterAddress();
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest(Address masterAddress) {
            ClientInvocation invocation = new ClientInvocation(client, createJoinJobRequest(), jobObjectName(getJobId()),
                    masterAddress);
            return new ExecutionFuture(invocation.invoke(), getJobId(), masterAddress);
        }

        @Override
        protected JobStatus sendJobStatusRequest() {
            return JetClientInstanceImpl.this.sendJobStatusRequest(getJobId(), true);
        }

        private ClientMessage createJoinJobRequest() {
            Data serializedDag = serializationService.toData(dag);
            Data serializedConfig = serializationService.toData(config);
            return JetSubmitJobCodec.encodeRequest(getJobId(), serializedDag, serializedConfig);
        }

    }

    private class TrackedJobImpl extends AbstractTrackedJobImpl {

        TrackedJobImpl(ILogger logger, long jobId) {
            super(logger, jobId);
        }

        @Override
        protected Address getMasterAddress() {
            return JetClientInstanceImpl.this.masterAddress();
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest(Address masterAddress) {
            ClientMessage request = JetJoinSubmittedJobCodec.encodeRequest(getJobId());
            ClientInvocation invocation = new ClientInvocation(client, request, jobObjectName(getJobId()), masterAddress);
            return new ExecutionFuture(invocation.invoke(), getJobId(), masterAddress);
        }

        @Override
        protected JobStatus sendJobStatusRequest() {
            return JetClientInstanceImpl.this.sendJobStatusRequest(getJobId(), false);
        }

    }

    private final class ExecutionFuture implements ICompletableFuture<Void> {

        private final ClientInvocationFuture future;
        private final long jobId;
        private final Address executionAddress;

        ExecutionFuture(ClientInvocationFuture future, long jobId, Address executionAddress) {
            this.future = future;
            this.jobId = jobId;
            this.executionAddress = executionAddress;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = future.cancel(true);
            if (!cancelled) {
                return false;
            }
            new ClientInvocation(client, JetCancelJobCodec.encodeRequest(jobId), jobObjectName(jobId), executionAddress)
                    .invoke().andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage clientMessage) {
                    //ignored
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.warning("Error cancelling job with jobId " + idToString(jobId), throwable);
                }
            });
            return true;
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public Void get() throws InterruptedException, ExecutionException {
            future.get();
            return null;
        }

        @Override
        public Void get(long timeout, @Nonnull TimeUnit unit)
                throws InterruptedException, ExecutionException, TimeoutException {
            future.get(timeout, unit);
            return null;
        }

        @Override
        public void andThen(ExecutionCallback<Void> callback) {
            future.andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage response) {
                    callback.onResponse(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            });
        }

        @Override
        public void andThen(ExecutionCallback<Void> callback, Executor executor) {
            future.andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage response) {
                    callback.onResponse(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            }, executor);
        }

    }
}
