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

package com.hazelcast.jet;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.DeploymentConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.impl.job.JobProxy;
import com.hazelcast.jet.impl.job.JobService;
import com.hazelcast.jet.impl.job.client.ClientJobProxy;
import com.hazelcast.jet.impl.statemachine.job.JobState;
import com.hazelcast.jet.job.Job;

import java.util.Set;

import static com.hazelcast.jet.impl.util.JetUtil.checkJobName;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Utility class for creating new Jet Jobs
 */
public final class JetEngine {
    private JetEngine() {
    }

    /**
     * Create a new job given a Hazelcast instance and name
     *
     * @param hazelcastInstance Hazelcast instance to use
     * @param name              name of the job
     * @param dag               Direct acyclic graph, which describes calculation flow
     * @return a new Jet job
     */
    public static Job getJob(HazelcastInstance hazelcastInstance, String name, DAG dag) {
        return getJob(hazelcastInstance, name, dag, new JobConfig());
    }

    /**
     * Create a new job given a Hazelcast instance, name and job configuration
     *
     * @param hazelcastInstance Hazelcast instance to use
     * @param name              name of the job
     * @param dag               Direct acyclic graph, which describes calculation flow
     * @param jobConfig         configuration for the job
     * @return a new Jet job
     */
    public static Job getJob(HazelcastInstance hazelcastInstance, String name, DAG dag, JobConfig jobConfig) {
        checkJobName(name);
        checkNotNull(dag, "DAG cannot be null");

        Job job = hazelcastInstance.getDistributedObject(JobService.SERVICE_NAME, name);

        Set<DeploymentConfig> deploymentConfigs = jobConfig.getDeploymentConfigs();

        // TODO: job init should be done in createDistributedObject
        if (job.getJobState() == JobState.NEW) {
            if (job instanceof JobProxy) {
                JobProxy jobProxy = (JobProxy) job;
                jobProxy.init(jobConfig);
                jobProxy.submit(dag, deploymentConfigs);
            } else {
                ClientJobProxy clientJobProxy = (ClientJobProxy) job;
                clientJobProxy.init(jobConfig);
                clientJobProxy.submit(dag, deploymentConfigs);
            }
        }

        return job;
    }
}
