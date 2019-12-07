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

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.impl.metrics.RawJobMetrics;
import com.hazelcast.jet.impl.operation.GetJobConfigOperation;
import com.hazelcast.jet.impl.operation.GetJobMetricsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.GetJobSubmissionTimeOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.ResumeJobOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.jet.impl.operation.TerminateJobOperation;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.impl.JobMetricsUtil.toJobMetrics;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static com.hazelcast.jet.impl.util.Util.getJetInstance;

/**
 * {@link Job} proxy on member.
 */
public class JobProxy extends AbstractJobProxy<NodeEngineImpl> {

    public JobProxy(NodeEngineImpl nodeEngine, long jobId) {
        super(nodeEngine, jobId);
    }

    public JobProxy(NodeEngineImpl engine, long jobId, DAG dag, JobConfig config) {
        super(engine, jobId, dag, config);
    }

    @Nonnull @Override
    public JobStatus getStatus() {
        try {
            return this.<JobStatus>invokeOp(new GetJobStatusOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Nonnull @Override
    public JobMetrics getMetrics() {
        try {
            List<RawJobMetrics> shards = this.<List<RawJobMetrics>>invokeOp(new GetJobMetricsOperation(getId())).get();
            return toJobMetrics(shards);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected CompletableFuture<Void> invokeSubmitJob(Data dag, JobConfig config) {
        return invokeOp(new SubmitJobOperation(getId(), dag, serializationService().toData(config)));
    }

    @Override
    protected CompletableFuture<Void> invokeJoinJob() {
        return invokeOp(new JoinSubmittedJobOperation(getId()));
    }

    @Override
    protected CompletableFuture<Void> invokeTerminateJob(TerminationMode mode) {
        return invokeOp(new TerminateJobOperation(getId(), mode));
    }

    @Override
    public void resume() {
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
        try {
            JetService jetService = container().getService(JetService.SERVICE_NAME);
            Operation operation = jetService.createExportSnapshotOperation(getId(), name, cancelJob);
            invokeOp(operation).get();
        } catch (Exception e) {
            throw rethrow(e);
        }
        return getJetInstance(container()).getJobStateSnapshot(name);
    }

    @Override
    protected long doGetJobSubmissionTime() {
        try {
            return this.<Long>invokeOp(new GetJobSubmissionTimeOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected JobConfig doGetJobConfig() {
        try {
            return this.<JobConfig>invokeOp(new GetJobConfigOperation(getId())).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @Override
    protected Address masterAddress() {
        Address masterAddress = container().getMasterAddress();
        if (masterAddress == null) {
            throw new IllegalStateException("Master address unknown: instance is not yet initialized or is shut down");
        }
        return masterAddress;
    }

    @Override
    protected SerializationService serializationService() {
        return container().getSerializationService();
    }

    @Override
    protected LoggingService loggingService() {
        return container().getLoggingService();
    }

    private <T> CompletableFuture<T> invokeOp(Operation op) {
        return container()
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, op, masterAddress())
                .invoke();
    }
}
