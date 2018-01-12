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

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobNotFoundException;
import com.hazelcast.jet.impl.operation.GetJobIdsByNameOperation;
import com.hazelcast.jet.impl.operation.GetJobIdsOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.ExceptionUtil.peel;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
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

    @Nonnull @Override
    public JetConfig getConfig() {
        return config;
    }

    @Nonnull @Override
    public Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config) {
        long jobId = uploadResourcesAndAssignId(config);
        return new JobProxy((NodeEngineImpl) nodeEngine, jobId, dag, config);
    }

    @Nonnull @Override
    public List<Job> getJobs() {
        Address masterAddress = nodeEngine.getMasterAddress();
        Future<Set<Long>> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsOperation(), masterAddress)
                .invoke();
        return uncheckCall(() ->
                future.get().stream().map(jobId -> new JobProxy((NodeEngineImpl) nodeEngine, jobId)).collect(toList())
        );
    }

    @Override
    public Job getJob(long jobId) {
        try {
            Job job = new JobProxy((NodeEngineImpl) nodeEngine, jobId);
            job.getStatus();
            return job;
        } catch (Exception e) {
            if (peel(e) instanceof JobNotFoundException) {
                return null;
            }
            throw e;
        }
    }

    @Nonnull @Override
    public List<Job> getJobs(@Nonnull String name) {
        return getJobIdsByName(name).stream()
                                    .map(jobId -> new JobProxy((NodeEngineImpl) nodeEngine, jobId))
                                    .collect(toList());
    }

    private List<Long> getJobIdsByName(String name) {
        Address masterAddress = nodeEngine.getMasterAddress();
        Future<List<Long>> future = nodeEngine
                .getOperationService()
                .createInvocationBuilder(JetService.SERVICE_NAME, new GetJobIdsByNameOperation(name), masterAddress)
                .invoke();

        return uncheckCall(future::get);
    }

}
