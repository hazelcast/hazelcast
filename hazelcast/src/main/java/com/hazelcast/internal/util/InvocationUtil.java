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

package com.hazelcast.internal.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.core.PartitionService;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.util.futures.ChainingFuture;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.Iterator;

import static com.hazelcast.util.IterableUtil.map;

/**
 * Utility methods for invocations.
 */
public final class InvocationUtil {

    private static final int WARMUP_SLEEPING_TIME_MILLIS = 10;

    private InvocationUtil() {
    }

    /**
     * Invoke operation on all cluster members.
     *
     * The invocation is serial: It iterates over all members starting from the oldest member to the youngest one.
     * If there is a cluster membership change while invoking then it will restart invocations on all members. This
     * implies the operation should be idempotent.
     *
     * If there is an exception - other than {@link com.hazelcast.core.MemberLeftException} or
     * {@link com.hazelcast.spi.exception.TargetNotMemberException} while invoking then the iteration
     * is interrupted and the exception is propagated to the caller.
     */
    public static ICompletableFuture<Object> invokeOnStableClusterSerial(NodeEngine nodeEngine,
                                                                         OperationFactory operationFactory,
                                                                         int maxRetries) {
        warmUpPartitions(nodeEngine);

        final OperationService operationService = nodeEngine.getOperationService();
        ClusterService clusterService = nodeEngine.getClusterService();

        RestartingMemberIterator memberIterator = new RestartingMemberIterator(clusterService, maxRetries);

        // we are going to iterate over all members and invoke an operation on each of them
        Iterator<ICompletableFuture<Object>> invocationIterator = map(
                memberIterator, new InvokeOnMemberFunction(operationFactory, operationService));

        ILogger logger = nodeEngine.getLogger(ChainingFuture.class);
        ExecutionService executionService = nodeEngine.getExecutionService();
        ManagedExecutorService executor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);

        // ChainingFuture uses the iterator to start invocations
        // it invokes on another member only when the previous invocation is completed (so invocations are serial)
        // the future itself completes only when the last invocation completes (or if there is an error)
        return new ChainingFuture<Object>(invocationIterator, executor, memberIterator, logger);
    }

    private static void warmUpPartitions(NodeEngine nodeEngine) {
        final PartitionService ps = nodeEngine.getHazelcastInstance().getPartitionService();
        for (Partition partition : ps.getPartitions()) {
            while (partition.getOwner() == null) {
                try {
                    Thread.sleep(WARMUP_SLEEPING_TIME_MILLIS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new HazelcastException("Thread interrupted while initializing a partition table", e);
                }
            }
        }
    }

    // IFunction extends Serializable, but this function is only executed locally
    @SerializableByConvention
    private static class InvokeOnMemberFunction implements IFunction<Member, ICompletableFuture<Object>> {

        private final OperationFactory operationFactory;
        private final OperationService operationService;

        InvokeOnMemberFunction(OperationFactory operationFactory, OperationService operationService) {
            this.operationFactory = operationFactory;
            this.operationService = operationService;
        }

        @Override
        public ICompletableFuture<Object> apply(Member member) {
            Address address = member.getAddress();
            Operation operation = operationFactory.createOperation();
            String serviceName = operation.getServiceName();

            return operationService.invokeOnTarget(serviceName, operation, address);
        }
    }
}
