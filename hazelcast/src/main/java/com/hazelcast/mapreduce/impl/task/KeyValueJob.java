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

package com.hazelcast.mapreduce.impl.task;

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.*;
import com.hazelcast.mapreduce.impl.operation.KeyValueJobOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.*;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyValueJob<KeyIn, ValueIn> extends AbstractJob<KeyIn, ValueIn> {

    private final NodeEngine nodeEngine;

    public KeyValueJob(String name, JobTracker jobTracker, NodeEngine nodeEngine,
                       KeyValueSource<KeyIn, ValueIn> keyValueSource) {
        super(name, jobTracker, keyValueSource);
        this.nodeEngine = nodeEngine;
    }

    @Override
    protected <T> CompletableFuture<T> invoke() {
        ExecutionService es = nodeEngine.getExecutionService();
        ManagedExecutorService mes = es.getExecutor(name);
        AbstractJobTracker jobTracker = (AbstractJobTracker) this.jobTracker;
        TrackableJobFuture<T> jobFuture = new TrackableJobFuture<T>(name, jobId, jobTracker, nodeEngine);
        if (jobTracker.registerTrackableJob(jobFuture)) {
            return startSupervisionTask(jobFuture);
        }
        throw new IllegalStateException("Could not register map reduce job");
    }

    private <T> CompletableFuture<T> startSupervisionTask(TrackableJobFuture<T> jobFuture) {
        ClusterService cs = nodeEngine.getClusterService();
        OperationService os = nodeEngine.getOperationService();
        PartitionService ps = nodeEngine.getPartitionService();

        Map<Address, List<KeyIn>> mappedKeys = MapReduceUtil.mapKeysToMember(ps, keys);
        Map<MemberImpl, InternalCompletableFuture> futures = new HashMap<MemberImpl, InternalCompletableFuture>();
        for (MemberImpl member : cs.getMemberList()) {
            List<KeyIn> keys = mappedKeys.get(member.getAddress());
            Operation operation = new KeyValueJobOperation<KeyIn, ValueIn>(name, jobId, chunkSize,
                    keys, predicate, keyValueSource, mapper, combinerFactory, reducerFactory);

            operation.setServiceName(MapReduceService.SERVICE_NAME);
            operation.setExecutorName(name);
            operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            if (cs.getThisAddress().equals(member.getAddress())) {
                os.runOperation(operation);
            } else {
                os.send(operation, member.getAddress());
            }
        }
        return jobFuture;
    }

}
