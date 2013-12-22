/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.impl.task.JobSupervisor;
import com.hazelcast.spi.ManagedService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.RemoteService;
import com.hazelcast.util.ConcurrencyUtil;
import com.hazelcast.util.ConstructorFunction;

import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MapReduceService implements ManagedService, RemoteService {

    public static final String SERVICE_NAME = "hz:impl:mapReduceService";

    private final ConstructorFunction<String, JobTracker> trackerConstructor = new ConstructorFunction<String, JobTracker>() {
        @Override
        public JobTracker createNew(String arg) {
            JobTrackerConfig jobTrackerConfig = config.findJobTrackerConfig(arg);
            return new NodeJobTracker(arg, jobTrackerConfig.getAsReadOnly(), nodeEngine, hazelcastInstance);
        }
    };

    private final ConstructorFunction<JobSupervisorKey, JobSupervisor> supervisorConstructor = new ConstructorFunction<JobSupervisorKey, JobSupervisor>() {
        @Override
        public JobSupervisor createNew(JobSupervisorKey arg) {
            JobTracker jobTracker = (JobTracker) createDistributedObject(arg.name);
            return new JobSupervisor(arg.name, arg.jobId, jobTracker);
        }
    };

    private final ConcurrentMap<String, JobTracker> jobTrackers = new ConcurrentHashMap<String, JobTracker>();
    private final ConcurrentMap<JobSupervisorKey, JobSupervisor> jobSupervisors = new ConcurrentHashMap<JobSupervisorKey, JobSupervisor>();
    private final HazelcastInstance hazelcastInstance;
    private final NodeEngine nodeEngine;
    private final Config config;

    public MapReduceService(Config config, NodeEngine nodeEngine,
                            HazelcastInstance hazelcastInstance) {
        this.config = config;
        this.nodeEngine = nodeEngine;
        this.hazelcastInstance = hazelcastInstance;
    }

    public JobTracker getJobTracker(String name) {
        return (JobTracker) createDistributedObject(name);
    }

    public JobSupervisor getJobSupervisor(String name, String jobId) {
        JobSupervisorKey key = new JobSupervisorKey(name, jobId);
        return ConcurrencyUtil.getOrPutIfAbsent(jobSupervisors, key, supervisorConstructor);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
    }

    @Override
    public void reset() {
    }

    @Override
    public void shutdown(boolean terminate) {
        for (JobTracker jobTracker : jobTrackers.values()) {
            jobTracker.destroy();
        }
        jobTrackers.clear();
    }

    @Override
    public DistributedObject createDistributedObject(String objectName) {
        return ConcurrencyUtil.getOrPutSynchronized(jobTrackers, objectName, jobTrackers, trackerConstructor);
    }

    @Override
    public void destroyDistributedObject(String objectName) {
        JobTracker jobTracker = jobTrackers.remove(objectName);
        if (jobTracker != null) {
            jobTracker.destroy();
        }
    }

    private static class JobSupervisorKey {
        private final String name;
        private final String jobId;

        private JobSupervisorKey(String name, String jobId) {
            this.name = name;
            this.jobId = jobId;
        }
    }

}
