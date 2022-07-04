/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.JetExportSnapshotCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobConfigCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobMetricsCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobStatusCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSubmissionTimeCodec;
import com.hazelcast.client.impl.protocol.codec.JetGetJobSuspensionCauseCodec;
import com.hazelcast.client.impl.protocol.codec.JetJoinSubmittedJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetResumeJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetSubmitJobCodec;
import com.hazelcast.client.impl.protocol.codec.JetTerminateJobCodec;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.exception.TargetNotMemberException;

import javax.annotation.Nonnull;
import java.util.UUID;
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
public class ClientJobProxy extends AbstractJobProxy<HazelcastClientInstanceImpl, UUID> {

    private static final long RETRY_DELAY_NS = MILLISECONDS.toNanos(200);
    private static final long RETRY_TIME_NS = SECONDS.toNanos(60);

    ClientJobProxy(HazelcastClientInstanceImpl client, long jobId, UUID coordinator) {
        super(client, jobId, coordinator);
    }

    ClientJobProxy(
            HazelcastClientInstanceImpl client,
            long jobId,
            boolean isLightJob,
            @Nonnull Object jobDefinition,
            @Nonnull JobConfig config
    ) {
        super(client, jobId, isLightJob, jobDefinition, config);
    }

    @Nonnull
    @Override
    public JobStatus getStatus0() {
        assert !isLightJob();
        return callAndRetryIfTargetNotFound(()  -> {
            ClientMessage request = JetGetJobStatusCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterId()).invoke().get();
            int jobStatusIndex = JetGetJobStatusCodec.decodeResponse(response);
            return JobStatus.values()[jobStatusIndex];
        });
    }

    @Nonnull
    @Override
    public JobSuspensionCause getSuspensionCause() {
        checkNotLightJob("suspensionCause");
        return callAndRetryIfTargetNotFound(()  -> {
            ClientMessage request = JetGetJobSuspensionCauseCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterId()).invoke().get();
            Data data = JetGetJobSuspensionCauseCodec.decodeResponse(response);
            return serializationService().toObject(data);
        });
    }

    @Nonnull
    @Override
    public JobMetrics getMetrics() {
        checkNotLightJob("metrics");
        return callAndRetryIfTargetNotFound(()  -> {
            ClientMessage request = JetGetJobMetricsCodec.encodeRequest(getId());
            ClientMessage response = invocation(request, masterId()).invoke().get();
            Data metricsData = JetGetJobMetricsCodec.decodeResponse(response);
            return toJobMetrics(serializationService().toObject(metricsData));
        });
    }

    @Override
    protected UUID findLightJobCoordinator() {
        ClientConnection connection = container().getConnectionManager().getRandomConnection();
        if (connection == null) {
            throw new JetException("The client isn't connected to the cluster");
        }

        return connection.getRemoteUuid();
    }

    @Override
    protected CompletableFuture<Void> invokeSubmitJob(Object jobDefinition, JobConfig config) {
        Data configData = serializationService().toData(config);
        Data jobDefinitionData = serializationService().toData(jobDefinition);
        ClientMessage request = JetSubmitJobCodec.encodeRequest(getId(), jobDefinitionData, configData, lightJobCoordinator);
        return invocation(request, coordinatorId()).invoke().thenApply(c -> null);
    }

    @Override
    protected CompletableFuture<Void> invokeJoinJob() {
        ClientMessage request = JetJoinSubmittedJobCodec.encodeRequest(getId(), lightJobCoordinator);
        ClientInvocation invocation = invocation(request, coordinatorId());
        // this invocation should never time out, as the job may be running for a long time
        invocation.setInvocationTimeoutMillis(Long.MAX_VALUE); // 0 is not supported
        return invocation.invoke().thenApply(c -> null);
    }

    @Override
    protected CompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        ClientMessage request = JetTerminateJobCodec.encodeRequest(getId(), mode.ordinal(), lightJobCoordinator);
        return invocation(request, coordinatorId()).invoke().thenApply(c -> null);
    }

    @Override
    public void resume() {
        checkNotLightJob("resume");
        ClientMessage request = JetResumeJobCodec.encodeRequest(getId());
        try {
            invocation(request, masterId()).invoke().get();
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
        checkNotLightJob("export snapshot");
        ClientMessage request = JetExportSnapshotCodec.encodeRequest(getId(), name, cancelJob);
        try {
            invocation(request, masterId()).invoke().get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return container().getJet().getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        return callAndRetryIfTargetNotFound(() -> {
            ClientMessage request = JetGetJobSubmissionTimeCodec.encodeRequest(getId(), lightJobCoordinator);
            ClientMessage response = invocation(request, coordinatorId()).invoke().get();
            return JetGetJobSubmissionTimeCodec.decodeResponse(response);
        });
    }

    @Override
    protected JobConfig doGetJobConfig() {
        return callAndRetryIfTargetNotFound(() -> {
            ClientMessage request = JetGetJobConfigCodec.encodeRequest(getId(), lightJobCoordinator);
            ClientMessage response = invocation(request, masterId()).invoke().get();
            Data data = JetGetJobConfigCodec.decodeResponse(response);
            return serializationService().toObject(data);
        });
    }

    @Nonnull @Override
    protected UUID masterId() {
        Member masterMember = container().getClientClusterService().getMasterMember();
        if (masterMember == null) {
            throw new IllegalStateException("Master isn't known");
        }
        return masterMember.getUuid();
    }

    @Override
    protected SerializationService serializationService() {
        return container().getSerializationService();
    }

    @Override
    protected LoggingService loggingService() {
        return container().getLoggingService();
    }

    @Override
    protected boolean isRunning() {
        return container().getLifecycleService().isRunning();
    }

    private ClientInvocation invocation(ClientMessage request, UUID invocationUuid) {
        return new ClientInvocation(
                container(), request, "jobId=" + getIdString(), invocationUuid);
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
