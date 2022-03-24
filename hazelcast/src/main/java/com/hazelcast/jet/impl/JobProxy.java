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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.JobSuspensionCause;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobMetricsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.GetJobSuspensionCauseOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

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
    public JobStatus getStatus0() {
        assert !isLightJob();
        try {
            return this.<JobStatus>invokeOp(new GetJobStatusOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull @Override
    public JobSuspensionCause getSuspensionCause() {
        checkNotLightJob("suspensionCause");
        try {
            return this.<JobSuspensionCause>invokeOp(new GetJobSuspensionCauseOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull @Override
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
        if (isLightJob()) {
            if (jobDefinition instanceof DAG) {
                ((DAG) jobDefinition).lock();
            }
            config.lock();
            return invokeOp(new SubmitJobOperation(getId(), jobDefinition, config, null, null, isLightJob(), null));
        }
        Data configData = serializationService().toData(config);
        Data jobDefinitionData = serializationService().toData(jobDefinition);
        return invokeOp(new SubmitJobOperation(getId(), null, null, jobDefinitionData, configData, isLightJob(), null));
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

    @Nonnull @Override
    protected Address masterId() {
        Address masterAddress = container().getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master address unknown: instance is not yet initialized or is shut down");
        }
        return masterAddress;
    }
}
