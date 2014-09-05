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

import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.task.KeyValueJob;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.executor.ExecutorType;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * {@link com.hazelcast.mapreduce.JobTracker} implementation for a node initiated map reduce job
 */
class NodeJobTracker
        extends AbstractJobTracker {

    private final CopyOnWriteArrayList<String> cancelledJobs = new CopyOnWriteArrayList<String>();

    NodeJobTracker(String name, JobTrackerConfig jobTrackerConfig, NodeEngine nodeEngine, MapReduceService mapReduceService) {

        super(name, jobTrackerConfig, nodeEngine, mapReduceService);

        ExecutionService es = nodeEngine.getExecutionService();
        InternalPartitionService ps = nodeEngine.getPartitionService();
        int maxThreadSize = jobTrackerConfig.getMaxThreadSize();
        if (maxThreadSize <= 0) {
            maxThreadSize = Runtime.getRuntime().availableProcessors();
        }
        int queueSize = jobTrackerConfig.getQueueSize();
        if (queueSize <= 0) {
            queueSize = ps.getPartitionCount() * 2;
        }

        try {
            String executorName = MapReduceUtil.buildExecutorName(name);
            es.register(executorName, maxThreadSize, queueSize, ExecutorType.CACHED);
        } catch (Exception ignore) {
            // After destroying the proxy and recreating it the executor
            // might already be registered, so we can ignore this exception.
            ILogger logger = nodeEngine.getLogger(NodeJobTracker.class);
            logger.finest("This is likely happened due to a previously cancelled job", ignore);
        }
    }

    @Override
    public <K, V> Job<K, V> newJob(KeyValueSource<K, V> source) {
        return new KeyValueJob<K, V>(name, this, nodeEngine, mapReduceService, source);
    }

    /*
     * Deactivated for now since feature moved to Hazelcast 3.3
    @Override
    public <K, V> ProcessJob<K, V> newProcessJob(KeyValueSource<K, V> source) {
        // TODO Implementation of process missing
        throw new UnsupportedOperationException("mapreduce process system not yet implemented");
    }*/

    public boolean registerJobSupervisorCancellation(String jobId) {
        return cancelledJobs.addIfAbsent(jobId);
    }

    public boolean unregisterJobSupervisorCancellation(String jobId) {
        return cancelledJobs.remove(jobId);
    }

}
