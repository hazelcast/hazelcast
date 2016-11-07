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

package com.hazelcast.jet.impl.job;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.DeploymentConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.counters.Accumulator;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.impl.statemachine.job.JobStateMachine;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.spi.AbstractDistributedObject;
import com.hazelcast.spi.NodeEngine;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class JobProxy extends AbstractDistributedObject<JobService> implements Job {
    private final String name;
    private final HazelcastInstance hazelcastInstance;
    private final JobStateMachine jobStateMachine;
    private final JobClusterService jobClusterService;

    public JobProxy(String name, JobService jobService, NodeEngine nodeEngine) {
        super(nodeEngine, jobService);
        this.name = name;
        hazelcastInstance = nodeEngine.getHazelcastInstance();
        jobStateMachine = new JobStateMachine(name);
        jobClusterService = new ServerJobClusterService(name, nodeEngine);
    }

    public void init(JobConfig config) {
        if (config == null) {
            config = JetUtil.resolveJobConfig(getNodeEngine(), name);
        }
        jobClusterService.init(config, jobStateMachine);
    }

    public void submit(DAG dag, Set<DeploymentConfig> deploymentConfigs) {
        deploy(deploymentConfigs);
        submit0(dag);
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
    public String getName() {
        return name;
    }

    @Override
    public JobState getJobState() {
        return jobStateMachine.currentState();
    }

    @Override
    public String getServiceName() {
        return JobService.SERVICE_NAME;
    }

    @Override
    public Map<String, Accumulator> getAccumulators() {
        return jobClusterService.getAccumulators();
    }

    @Override
    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    private void deploy(Set<DeploymentConfig> deploymentConfigs) {
        jobClusterService.deploy(deploymentConfigs, jobStateMachine);
    }

    private void submit0(final DAG dag) {
        jobClusterService.submitDag(dag, jobStateMachine);
    }
}
