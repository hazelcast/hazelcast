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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.client.protocol.codec.JetExportSnapshotCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobMetricsCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobStatusCodec.ResponseParameters;
import com.hazelcast.jet.impl.client.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetResumeJobCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.jet.impl.client.protocol.codec.JetTerminateJobCodec;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * {@link Job} proxy on client.
 */
public class ClientJobProxy extends AbstractJobProxy<JetClientInstanceImpl> {

    private static final long RETRY_DELAY_NS = MILLISECONDS.toNanos(200);
    private static final long RETRY_TIME_NS = SECONDS.toNanos(60);

    ClientJobProxy(JetClientInstanceImpl client, long jobId) {
        super(client, jobId);
    }

    ClientJobProxy(JetClientInstanceImpl client, long jobId, DAG dag, JobConfig config) {
        super(client, jobId, dag, config);
    }

    @Nonnull
    @Override
    public JobStatus getStatus() {
        return callAndRetryIfTargetNotFound(()  -> {
            ClientMessage request = JetGetJobStatusCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            ResponseParameters parameters = JetGetJobStatusCodec.decodeResponse(response);
            return JobStatus.values()[parameters.response];
        });
    }

    @Nonnull
    @Override
    public JobMetrics getMetrics() {
        return callAndRetryIfTargetNotFound(()  -> {
            ClientMessage request = JetGetJobMetricsCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            JetGetJobMetricsCodec.ResponseParameters parameters = JetGetJobMetricsCodec.decodeResponse(response);
            return toJobMetrics(serializationService().toObject(parameters.response));
        });
    }

    @Override
    protected CompletableFuture<Void> invokeSubmitJob(Data dag, JobConfig config) {
        Data configData = serializationService().toData(config);
        ClientMessage request = JetSubmitJobCodec.encodeRequest(getId(), dag, configData);
        return invocation(request, masterAddress()).invoke().thenApply(c -> null);
    }

    @Override
    protected CompletableFuture<Void> invokeJoinJob() {
        ClientMessage request = JetJoinSubmittedJobCodec.encodeRequest(getId());
        ClientInvocation invocation = invocation(request, masterAddress());
        // this invocation should never time out, as the job may be running for a long time
        invocation.setInvocationTimeoutMillis(Long.MAX_VALUE); // 0 is not supported
        return invocation.invoke().thenApply(c -> null);
    }

    @Override
    protected CompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        ClientMessage request = JetTerminateJobCodec.encodeRequest(getId(), mode.ordinal());
        return invocation(request, masterAddress()).invoke().thenApply(c -> null);
    }

    @Override
    public void resume() {
        ClientMessage request = JetResumeJobCodec.encodeRequest(getId());
        try {
            invocation(request, masterAddress()).invoke().get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public JobStateSnapshot cancelAndExportSnapshot(String name) {
        return doExportSnapshot(name, true);
    }

    @Override
    public JobStateSnapshot exportSnapshot(String name) {
        return doExportSnapshot(name, false);
    }

    private JobStateSnapshot doExportSnapshot(String name, boolean cancelJob) {
        ClientMessage request = JetExportSnapshotCodec.encodeRequest(getId(), name, cancelJob);
        try {
            invocation(request, masterAddress()).invoke().get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return container().getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        return callAndRetryIfTargetNotFound(() -> {
            ClientMessage request = JetGetJobSubmissionTimeCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            return JetGetJobSubmissionTimeCodec.decodeResponse(response).response;
        });
    }

    @Override
    protected JobConfig doGetJobConfig() {
        return callAndRetryIfTargetNotFound(() -> {
            ClientMessage request = JetGetJobConfigCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterAddress()).invoke().get();
            Data data = JetGetJobConfigCodec.decodeResponse(response).response;
            return serializationService().toObject(data);
        });
    }

    @Override
    protected Address masterAddress() {
        Optional<Member> first = container().getCluster().getMembers().stream().findFirst();
        return first.orElseThrow(() -> new IllegalStateException("No members found in cluster")).getAddress();
    }

    @Override
    protected SerializationService serializationService() {
        return container().getHazelcastClient().getSerializationService();
    }

    @Override
    protected LoggingService loggingService() {
        return container().getHazelcastClient().getLoggingService();
    }

    private ClientInvocation invocation(ClientMessage request, Address invocationAddr) {
        return new ClientInvocation(
                container().getHazelcastClient(), request, "jobId=" + getIdString(), invocationAddr
        );
    }

    private <T> T callAndRetryIfTargetNotFound(Callable<T> action) {
        long timeLimit = System.nanoTime() + RETRY_TIME_NS;
        for (;;) {
            try {
                return action.call();
            } catch (Exception e) {
                if (System.nanoTime() < timeLimit
                        && e instanceof ExecutionException
                        && e.getCause() instanceof TargetNotMemberException
                ) {
                    // ignore the TargetNotMemberException and retry with new master
                    LockSupport.parkNanos(RETRY_DELAY_NS);
                    continue;
                }
                throw rethrow(e);
            }
        }
    }

}
