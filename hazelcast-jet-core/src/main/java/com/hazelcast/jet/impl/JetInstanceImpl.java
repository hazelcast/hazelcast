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

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.jet.impl.operation.GetJobStatusOperation;
import com.hazelcast.jet.impl.operation.JoinSubmittedJobOperation;
import com.hazelcast.jet.impl.operation.SubmitJobOperation;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static java.util.stream.Collectors.toList;

/**
 * Member-side {@code JetInstance} implementation
 */
public class JetInstanceImpl extends AbstractJetInstance {
    private final NodeEngine nodeEngine;
    private final JetConfig config;

    public JetInstanceImpl(HazelcastInstanceImpl hazelcastInstance, JetConfig config) {
        super(hazelcastInstance);
        this.nodeEngine = hazelcastInstance.node.getNodeEngine();
        this.config = config;
    }

    @Override
    public JetConfig getConfig() {
        return config;
    }

    @Override
    public Job newJob(DAG dag) {
        ILogger logger = nodeEngine.getLogger(SubmittedJobImpl.class);
        SubmittedJobImpl job = new SubmittedJobImpl(this, logger, dag, new JobConfig());
        job.init();
        return job;
    }

    @Override
    public Job newJob(DAG dag, JobConfig config) {
        ILogger logger = nodeEngine.getLogger(SubmittedJobImpl.class);
        SubmittedJobImpl job = new SubmittedJobImpl(this, logger, dag, config);
        job.init();
        return job;
    }

    @Override
    public Collection<Job> getJobs() {
        Address masterAddress = nodeEngine.getMasterAddress();
        OperationService operationService = nodeEngine.getOperationService();
        InternalCompletableFuture<Set<Long>> future = operationService
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsOperation(), masterAddress).invoke();

        Set<Long> jobIds;
        try {
            jobIds = future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw rethrow(e);
        } catch (ExecutionException e) {
            throw rethrow(e);
        }

        List<Job> jobs = jobIds.stream().map(jobId ->
                new TrackedJobImpl(nodeEngine.getLogger(TrackedJobImpl.class), jobId))
                               .collect(toList());

        jobs.forEach(job -> ((TrackedJobImpl) job).init());

        return jobs;
    }

    private JobStatus sendJobStatusRequest(long jobId) {
        try {
            Operation op = new GetJobStatusOperation(jobId);
            OperationService operationService = nodeEngine.getOperationService();
            InternalCompletableFuture<JobStatus> f = operationService
                    .createInvocationBuilder(JetService.SERVICE_NAME, op, nodeEngine.getMasterAddress()).invoke();

            return f.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private class SubmittedJobImpl extends AbstractSubmittedJobImpl {

        SubmittedJobImpl(JetInstance jetInstance, ILogger logger, DAG dag, JobConfig config) {
            super(jetInstance, logger, dag, config);
        }

        @Override
        protected Address getMasterAddress() {
            return nodeEngine.getMasterAddress();
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest(Address masterAddress) {
            Data serializedDag = nodeEngine.getSerializationService().toData(dag);
            Operation op = new SubmitJobOperation(getJobId(), serializedDag, config);
            return nodeEngine.getOperationService()
                             .createInvocationBuilder(JetService.SERVICE_NAME, op, masterAddress)
                             .invoke();
        }

        @Override
        protected JobStatus sendJobStatusRequest() {
            return JetInstanceImpl.this.sendJobStatusRequest(getJobId());
        }

    }

    private class TrackedJobImpl extends AbstractTrackedJobImpl {

        TrackedJobImpl(ILogger logger, long jobId) {
            super(logger, jobId);
        }

        @Override
        protected Address getMasterAddress() {
            return nodeEngine.getMasterAddress();
        }

        @Override
        protected ICompletableFuture<Void> sendJoinRequest(Address masterAddress) {
            Operation op = new JoinSubmittedJobOperation(getJobId());
            return nodeEngine.getOperationService()
                             .createInvocationBuilder(JetService.SERVICE_NAME, op, masterAddress)
                             .invoke();
        }

        @Override
        protected JobStatus sendJobStatusRequest() {
            return JetInstanceImpl.this.sendJobStatusRequest(getJobId());
        }

    }

}
