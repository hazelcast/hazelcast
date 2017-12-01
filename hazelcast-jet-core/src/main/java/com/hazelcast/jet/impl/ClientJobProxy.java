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
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.Member;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.serialization.SerializationService;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.jet.impl.util.Util.idToString;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;

/**
 * {@link com.hazelcast.jet.Job} proxy on client.
 */
public class ClientJobProxy extends AbstractJobProxy<HazelcastClientInstanceImpl> {

    ClientJobProxy(HazelcastClientInstanceImpl client, long jobId) {
        super(client, jobId);
    }

    ClientJobProxy(HazelcastClientInstanceImpl client, long jobId, DAG dag, JobConfig config) {
        super(client, jobId, dag, config);
    }

    @Nonnull @Override
    public JobStatus getJobStatus() {
        ClientMessage request = JetGetJobStatusCodec.encodeRequest(getJobId(), shouldRetryJobStatus());
        return uncheckCall(() -> {
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            Data statusData = JetGetJobStatusCodec.decodeResponse(response).response;
            return serializationService().toObject(statusData);
        });
    }

    @Override
    protected ICompletableFuture<Void> invokeSubmitJob(Data dag, JobConfig config) {
        Data configData = serializationService().toData(config);
        ClientMessage request = JetSubmitJobCodec.encodeRequest(getJobId(), dag, configData);
        Address target = masterAddress();
        return new CancellableFuture<>(invocation(request, target).invoke(), target);
    }

    @Override
    protected ICompletableFuture<Void> invokeJoinJob() {
        ClientMessage request = JetJoinSubmittedJobCodec.encodeRequest(getJobId());
        Address target = masterAddress();
        return new CancellableFuture<>(invocation(request, target).invoke(), target);
    }

    @Override
    protected Address masterAddress() {
        Optional<Member> first = container().getCluster().getMembers().stream().findFirst();
        return first.orElseThrow(() -> new IllegalStateException("No members found in cluster")).getAddress();
    }

    @Override
    protected SerializationService serializationService() {
        return container().getSerializationService();
    }

    @Override
    protected LoggingService loggingService() {
        return container().getLoggingService();
    }

    private ClientInvocation invocation(ClientMessage request, Address invocationAddr) {
        return new ClientInvocation(container(), request, jobName(), invocationAddr);
    }

    private String jobName() {
        return "jobId=" + idToString(getJobId());
    }

    /**
     * Decorator for execution future which makes it cancellable
     */
    private class CancellableFuture<T> implements ICompletableFuture<Void> {

        private final ICompletableFuture<T> future;
        private final Address executionAddress;

        CancellableFuture(ICompletableFuture<T> future, Address invocationTarget) {
            this.future = future;
            this.executionAddress = invocationTarget;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = future.cancel(true);
            if (!cancelled) {
                return false;
            }
            cancelJob();
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
            future.andThen(new ExecutionCallback<T>() {
                @Override
                public void onResponse(T response) {
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
            future.andThen(new ExecutionCallback<T>() {
                @Override
                public void onResponse(T response) {
                    callback.onResponse(null);
                }

                @Override
                public void onFailure(Throwable t) {
                    callback.onFailure(t);
                }
            }, executor);
        }

        private void cancelJob() {
            invocation(JetCancelJobCodec.encodeRequest(getJobId()), executionAddress)
                    .invoke()
                    .andThen(new ExecutionCallback<ClientMessage>() {
                        @Override
                        public void onResponse(ClientMessage clientMessage) {
                            //ignored
                        }

                        @Override
                        public void onFailure(Throwable throwable) {
                            getLogger().warning("Error cancelling job with jobId " + idToString(getJobId()), throwable);
                        }
                    });
        }
    }
}
