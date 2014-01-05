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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.core.CompletableFuture;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.impl.operation.TrackedOperationFactory;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.util.ValidationUtil.isNotNull;

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
        return es.asCompletableFuture(mes.submit(new TrackedJob<T>()));
    }

    private class TrackedJob<T>
            implements TrackableJob<T> {

        private TrackedJob() {
            ((AbstractJobTracker) jobTracker).registerTrackableJob(this);
        }

        @Override
        public JobTracker getJobTracker() {
            return jobTracker;
        }

        @Override
        public String getJobId() {
            return jobId;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public T call() throws Exception {
            ClusterService cs = nodeEngine.getClusterService();
            OperationService os = nodeEngine.getOperationService();
            PartitionService ps = nodeEngine.getPartitionService();
            SerializationService ss = nodeEngine.getSerializationService();

            Map<Integer, List<KeyIn>> mappedKeys = MapReduceUtil.mapKeys(ps, keys);
            TrackedOperationFactory<KeyIn, ValueIn> factory =
                    new TrackedOperationFactory<KeyIn, ValueIn>(name, jobId, keyValueSource);

            Map<MemberImpl, InternalCompletableFuture> futures = new HashMap<MemberImpl, InternalCompletableFuture>();
            for (MemberImpl member : cs.getMemberList()) {
                os.invokeOnTarget()
            }

            return (T) os.invokeOnAllPartitions(MapReduceService.SERVICE_NAME, factory);
        }
    }

    private static class NonBlockingCompletableFuture<V> extends AbstractCompletableFuture<V> {

        private NonBlockingCompletableFuture(NodeEngine nodeEngine) {
            super(nodeEngine);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            long remaining = Clock.currentTimeMillis() + unit.toMillis(timeout);
            for (;;) {

            }
            return (V) getResult();
        }
    }
}
