/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.AddJobStatusListenerOperation;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobMetricsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.GetJobSuspensionCauseOperation;
import com.hazelcast.jet.impl.operation.IsJobUserCancelledOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.jet.impl.operation.UpdateJobConfigOperation;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.cluster.Versions.V5_3;
import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * {@link Job} proxy on member.
 */
public class JobProxy extends AbstractJobProxy<NodeEngineImpl, Address> {
    public JobProxy(NodeEngineImpl nodeEngine, long jobId, Address coordinator) {
        super(nodeEngine, jobId, coordinator);
    }

    public JobProxy(NodeEngineImpl nodeEngine,
                    long jobId,
                    boolean isLightJob,
                    Object jobDefinition,
                    JobConfig config,
                    @Nullable Subject subject) {
        super(nodeEngine, jobId, isLightJob, jobDefinition, config, subject);
    }

    @Nonnull
    @Override
    protected JobStatus getStatus0() {
        assert !isLightJob();
        try {
            return this.<JobStatus>invokeOp(new GetJobStatusOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected boolean isUserCancelled0() {
        assert !isLightJob();
        try {
            return this.<Boolean>invokeOp(new IsJobUserCancelledOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull
    @Override
    public JobSuspensionCause getSuspensionCause() {
        checkNotLightJob("suspensionCause");
        try {
            return this.<JobSuspensionCause>invokeOp(new GetJobSuspensionCauseOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull
    @Override
    public JobMetrics getMetrics() {
        checkNotLightJob("metrics");
        try {
            List<RawJobMetrics> shards = this.<List<RawJobMetrics>>invokeOp(new GetJobMetricsOperation(getId())).get();
            return toJobMetrics(shards);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected Address findLightJobCoordinator() {
        // If a light job is submitted from a member, it's always coordinated locally.
        // This is important for SQL jobs running in mixed-version clusters - the job DAG
        // was created locally and uses features available to the local member version.
        // A lite member can also coordinate.
        return container().getThisAddress();
    }

    @Override
    protected CompletableFuture<Void> invokeSubmitJob(Object jobDefinition, JobConfig config) {
        boolean serialize = true;
        if (isLightJob()) {
            if (jobDefinition instanceof DAG) {
                DAG dag = (DAG) jobDefinition;
                dag.lock();
                serialize = dag.vertices().stream().anyMatch(v -> !v.getMetaSupplier().isReusable());
            }
            config.lock();
        }
        if (serialize) {
            Data configData = serializationService().toData(config);
            Data jobDefinitionData = serializationService().toData(jobDefinition);
            return invokeOp(new SubmitJobOperation(
                    getId(), null, null, jobDefinitionData, configData, isLightJob(), subject));
        }
        return invokeOp(new SubmitJobOperation(
                getId(), jobDefinition, config, null, null, isLightJob(), subject));
    }

    @Override
    protected CompletableFuture<Void> invokeJoinJob() {
        return invokeOp(new JoinSubmittedJobOperation(getId(), isLightJob()));
    }

    @Override
    protected CompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        return invokeOp(new TerminateJobOperation(getId(), mode, isLightJob()));
    }

    @Override
    public void resume() {
        checkNotLightJob("resume");
        try {
            invokeOp(new ResumeJobOperation(getId())).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    protected JobStateSnapshot doExportSnapshot(String name, boolean cancelJob) {
        checkNotLightJob("export snapshot");
        JetServiceBackend jetServiceBackend = container().getService(JetServiceBackend.SERVICE_NAME);
        try {
            Operation operation = jetServiceBackend.createExportSnapshotOperation(getId(), name, cancelJob);
            invokeOp(operation).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
        return jetServiceBackend.getJet().getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        try {
            return this.<Long>invokeOp(new GetJobSubmissionTimeOperation(getId(), isLightJob())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected JobConfig doGetJobConfig() {
        try {
            return this.<JobConfig>invokeOp(new GetJobConfigOperation(getId(), isLightJob())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected JobConfig doUpdateJobConfig(@Nonnull DeltaJobConfig deltaConfig) {
        try {
            return this.<JobConfig>invokeOp(new UpdateJobConfigOperation(getId(), deltaConfig)).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
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
        return container().isRunning();
    }

    private <T> CompletableFuture<T> invokeOp(Operation op) {
        return container()
                .getOperationService()
                .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, coordinatorId())
                .invoke();
    }

    @Nonnull
    @Override
    protected Address masterId() {
        Address masterAddress = container().getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master address unknown: instance is not yet initialized or is shut down");
        }
        return masterAddress;
    }

    @Override
    protected UUID doAddStatusListener(@Nonnull JobStatusListener listener) {
        checkJobStatusListenerSupported(container());
        try {
            JobEventService jobEventService = container().getService(JobEventService.SERVICE_NAME);
            Registration registration = jobEventService.prepareRegistration(getId(), listener, false);
            return this.<UUID>invokeOp(new AddJobStatusListenerOperation(getId(), isLightJob(), registration)).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    public boolean removeStatusListener(@Nonnull UUID id) {
        checkJobStatusListenerSupported(container());
        JobEventService jobEventService = container().getService(JobEventService.SERVICE_NAME);
        return jobEventService.removeEventListener(getId(), id);
    }

    public static void checkJobStatusListenerSupported(NodeEngine nodeEngine) {
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(V5_3)) {
            throw new UnsupportedOperationException("Job status listener is not supported.");
        }
    }
}
