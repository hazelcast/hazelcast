/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.job.client;

import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.DeploymentConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.impl.job.JobClusterService;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class ClientJobProxy extends ClientProxy implements Job {
    private final JobStateMachine jobStateMachine;
    private JobClusterService jobClusterService;

    public ClientJobProxy(String serviceName, String name) {
        super(serviceName, name);
        jobStateMachine = new JobStateMachine(name);
    }

    protected void onInitialize() {
        jobClusterService = new ClientJobClusterService(getClient(), name);
    }

    public void init(JobConfig config) {
        if (config == null) {
            config = JetUtil.resolveJobConfig(getClient(), name);
        }
        jobClusterService.init(config, jobStateMachine);
    }

    public void submit(DAG dag, Set<DeploymentConfig> deploymentConfigs) {
        deploy(deploymentConfigs);
        submit0(dag);
    }

    private void deploy(Set<DeploymentConfig> deploymentConfigs) {
        jobClusterService.deploy(deploymentConfigs, jobStateMachine);
    }

    private void submit0(final DAG dag) {
        jobClusterService.submitDag(dag, jobStateMachine);
    }

    @Override
    public JobState getJobState() {
        return jobStateMachine.currentState();
    }

    @Override
    public Future execute() {
        return jobClusterService.execute(jobStateMachine);
    }

    @Override
    public Future interrupt() {
        return jobClusterService.interrupt(jobStateMachine);
    }

    @Override
    protected boolean preDestroy() {
        try {
            jobClusterService.destroy(jobStateMachine);
            return true;
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    @Override
    public Map<String, Accumulator> getAccumulators() {
        return jobClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return getClient();
    }
}
