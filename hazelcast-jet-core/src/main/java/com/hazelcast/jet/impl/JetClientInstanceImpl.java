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
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec.ResponseParameters;
import com.hazelcast.client.impl.protocol.codec.JetJoinJobCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.idToString;

/**
 * Client-side {@code JetInstance} implementation
 */
public class JetClientInstanceImpl extends AbstractJetInstance {

    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;

    public JetClientInstanceImpl(HazelcastClientInstanceImpl hazelcastInstance) {
        super(hazelcastInstance);
        this.client = hazelcastInstance;
        this.logger = hazelcastInstance.getLoggingService().getLogger(JetInstance.class);

        ExceptionUtil.registerJetExceptions(hazelcastInstance.getClientExceptionFactory());
    }

    @Override
    public JetConfig getConfig() {
        throw new UnsupportedOperationException("Jet Configuration is not available on the client");
    }

    @Override
    public Job newJob(DAG dag) {
        JobImpl job = new JobImpl(dag, new JobConfig());
        job.init();
        return job;
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        JobImpl job = new JobImpl(dag, config);
        job.init();
        return job;
    }

    private class JobImpl extends AbstractJobImpl {

        JobImpl(DAG dag, JobConfig config) {
            super(JetClientInstanceImpl.this, dag, config);
        }

        @Override
        protected Address getMasterAddress() {
            Set<Member> members = client.getCluster().getMembers();
            Member master = members.iterator().next();
            return master.getAddress();
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest(Address masterAddress) {
            ClientInvocation invocation = new ClientInvocation(client, createJoinJobRequest(), masterAddress);
            return new ExecutionFuture(invocation.invoke(), getJobId(), masterAddress);
        }

        @Override
        protected JobStatus sendJobStatusRequest() {
            Address masterAddress = getMasterAddress();
            ClientMessage request = JetGetJobStatusCodec.encodeRequest(getJobId());
            ClientInvocation invocation = new ClientInvocation(client, request, masterAddress);
            try {
                ClientMessage clientMessage = invocation.invoke().get();
                ResponseParameters response = JetGetJobStatusCodec.decodeResponse(clientMessage);
                return JetClientInstanceImpl.this.client.getSerializationService().toObject(response.response);
            } catch (Exception e) {
                throw rethrow(e);
            }
        }

        private ClientMessage createJoinJobRequest() {
            SerializationService serializationService = client.getSerializationService();
            Data dag = serializationService.toData(getDAG());
            Data jobConfig = serializationService.toData(getConfig());
            return JetJoinJobCodec.encodeRequest(getJobId(), dag, jobConfig);
        }

    }

    private final class ExecutionFuture implements ICompletableFuture<Void> {

        private final ClientInvocationFuture future;
        private final long executionId;
        private final Address executionAddress;

        ExecutionFuture(ClientInvocationFuture future, long executionId, Address executionAddress) {
            this.future = future;
            this.executionId = executionId;
            this.executionAddress = executionAddress;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = future.cancel(true);
            if (!cancelled) {
                return false;
            }
            new ClientInvocation(client, JetCancelJobCodec.encodeRequest(executionId), executionAddress)
                    .invoke().andThen(new ExecutionCallback<ClientMessage>() {
                @Override
                public void onResponse(ClientMessage clientMessage) {
                    //ignored
                }

                @Override
                public void onFailure(Throwable throwable) {
                    logger.warning("Error cancelling job with executionId " + idToString(executionId), throwable);
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
