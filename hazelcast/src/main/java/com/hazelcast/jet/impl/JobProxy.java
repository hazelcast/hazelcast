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
import com.hazelcast.jet.JobStatusListener;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.DeltaJobConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.operation.AddJobStatusListenerOperation;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobMetricsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.GetJobSuspensionCauseOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.UpdateJobConfigOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.Registration;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.internal.cluster.Versions.V5_3;
import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;

/**
 * {@link Job} proxy on member.
 */
public class JobProxy extends AbstractJobProxy<NodeEngineImpl, Address> {
    public JobProxy(NodeEngineImpl nodeEngine, long jobId, Address coordinator) {
        super(nodeEngine, jobId, coordinator);
    }

    public JobProxy(
            NodeEngineImpl engine,
            long jobId,
            boolean isLightJob,
            @Nonnull Object jobDefinition,
            @Nonnull JobConfig config
    ) {
        super(engine, jobId, isLightJob, jobDefinition, config);
    }

    @Nonnull @Override
    protected JobStatus getStatus1() {
        return invoke(new GetJobStatusOperation(getId(), isLightJob()));
    }

    @Nonnull @Override
    public JobSuspensionCause getSuspensionCause() {
        checkNotLightJob("suspensionCause");
        return invoke(new GetJobSuspensionCauseOperation(getId()));
    }

    @Nonnull @Override
    public JobMetrics getMetrics() {
        checkNotLightJob("metrics");
        return toJobMetrics(invoke(new GetJobMetricsOperation(getId())));
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
            return invokeAsync(new SubmitJobOperation(
                    getId(), null, null, jobDefinitionData, configData, isLightJob(), null));
        }
        return invokeAsync(new SubmitJobOperation(
                getId(), jobDefinition, config, null, null, isLightJob(), null));
    }

    @Override
    protected CompletableFuture<Void> invokeJoinJob() {
        return invokeAsync(new JoinSubmittedJobOperation(getId(), isLightJob()));
    }

    @Override
    protected CompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        return invokeAsync(new TerminateJobOperation(getId(), mode, isLightJob()));
    }

    @Override
    public void resume() {
        checkNotLightJob("resume");
        invoke(new ResumeJobOperation(getId()));
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
        JetServiceBackend jetServiceBackend = container().getService(JetServiceBackend.SERVICE_NAME);
        invoke(jetServiceBackend.createExportSnapshotOperation(getId(), name, cancelJob));
        return jetServiceBackend.getJet().getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        return invoke(new GetJobSubmissionTimeOperation(getId(), isLightJob()));
    }

    @Override
    protected JobConfig doGetJobConfig() {
        return invoke(new GetJobConfigOperation(getId(), isLightJob()));
    }

    @Override
    protected JobConfig doUpdateJobConfig(@Nonnull DeltaJobConfig deltaConfig) {
        return invoke(new UpdateJobConfigOperation(getId(), deltaConfig));
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
    protected boolean isContainerRunning() {
        return container().isRunning();
    }

    @Nonnull @Override
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
        JobEventService jobEventService = container().getService(JobEventService.SERVICE_NAME);
        Registration registration = jobEventService.prepareRegistration(getId(), listener, false);
        return invoke(new AddJobStatusListenerOperation(getId(), isLightJob(), registration));
    }

    @Override
    public boolean removeStatusListener(@Nonnull UUID id) {
        checkJobStatusListenerSupported(container());
        JobEventService jobEventService = container().getService(JobEventService.SERVICE_NAME);
        return jobEventService.removeEventListener(getId(), id);
    }

    private <T> T invoke(Operation op) {
        return joinAndInvoke(() -> this.<T>invokeAsync(op).get());
    }

    private <T> CompletableFuture<T> invokeAsync(Operation op) {
        return container()
                .getOperationService()
                .createInvocationBuilder(JetServiceBackend.SERVICE_NAME, op, coordinatorId())
                .invoke();
    }

    public static void checkJobStatusListenerSupported(NodeEngine nodeEngine) {
        if (nodeEngine.getClusterService().getClusterVersion().isLessThan(V5_3)) {
            throw new UnsupportedOperationException("Job status listener is not supported.");
        }
    }
}
