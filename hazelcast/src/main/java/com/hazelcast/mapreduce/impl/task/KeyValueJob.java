/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.config.JobTrackerConfig;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.mapreduce.Collator;
import com.hazelcast.mapreduce.JobCompletableFuture;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.AbstractJob;
import com.hazelcast.mapreduce.impl.AbstractJobTracker;
import com.hazelcast.mapreduce.impl.MapReduceService;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.mapreduce.impl.operation.StartProcessingJobOperation;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.UuidUtil;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.mapreduce.impl.MapReduceUtil.executeOperation;

/**
 * This class is the internal representation of a map reduce job. It is responsible for requesting the
 * preparation, processing and sets up the future to retrieve the value in a fully reactive way.
 *
 * @param <KeyIn>   type of the key
 * @param <ValueIn> type of the value
 */
public class KeyValueJob<KeyIn, ValueIn>
        extends AbstractJob<KeyIn, ValueIn> {

    private final NodeEngine nodeEngine;
    private final MapReduceService mapReduceService;

    public KeyValueJob(String name, AbstractJobTracker jobTracker, NodeEngine nodeEngine, MapReduceService mapReduceService,
                       KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        super(name, jobTracker, keyValueSource);
        this.nodeEngine = nodeEngine;
        this.mapReduceService = mapReduceService;
    }

    @Override
    protected <T> JobCompletableFuture<T> invoke(Collator collator) {
        ClusterService clusterService = nodeEngine.getClusterService();
        if (clusterService.getSize(MemberSelectors.DATA_MEMBER_SELECTOR) == 0) {
            throw new IllegalStateException("Could not register map reduce job since there are no nodes owning a partition");
        }

        String jobId = UuidUtil.newUnsecureUuidString();

        AbstractJobTracker jobTracker = (AbstractJobTracker) this.jobTracker;
        TrackableJobFuture<T> jobFuture = new TrackableJobFuture<T>(name, jobId, jobTracker, nodeEngine, collator);
        if (jobTracker.registerTrackableJob(jobFuture)) {
            return startSupervisionTask(jobFuture, jobId);
        }

        throw new IllegalStateException("Could not register map reduce job");
    }

    private <T> JobCompletableFuture<T> startSupervisionTask(TrackableJobFuture<T> jobFuture, String jobId) {

        AbstractJobTracker jobTracker = (AbstractJobTracker) this.jobTracker;
        JobTrackerConfig config = jobTracker.getJobTrackerConfig();
        boolean communicateStats = config.isCommunicateStats();
        if (chunkSize == -1) {
            chunkSize = config.getChunkSize();
        }
        if (topologyChangedStrategy == null) {
            topologyChangedStrategy = config.getTopologyChangedStrategy();
        }

        ClusterService clusterService = nodeEngine.getClusterService();
        for (Member member : clusterService.getMembers(KeyValueJobOperation.MEMBER_SELECTOR)) {
            Operation operation = new KeyValueJobOperation<KeyIn, ValueIn>(name, jobId, chunkSize, keyValueSource, mapper,
                    combinerFactory, reducerFactory, communicateStats, topologyChangedStrategy);

            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }

        // After we prepared all the remote systems we can now start the processing
        for (Member member : clusterService.getMembers(DATA_MEMBER_SELECTOR)) {
            Operation operation = new StartProcessingJobOperation<KeyIn>(name, jobId, keys, predicate);
            executeOperation(operation, member.getAddress(), mapReduceService, nodeEngine);
        }
        return jobFuture;
    }

}
