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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.util.futures.ChainingFuture;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.SerializableByConvention;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.executor.CompletedFuture;
import com.hazelcast.util.executor.ManagedExecutorService;
import com.hazelcast.util.function.Supplier;

import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.IterableUtil.map;

/**
 * Utility methods for invocations.
 */
public final class InvocationUtil {

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
                                                                         Supplier<Operation> operationSupplier,
                                                                         int maxRetries) {

        ClusterService clusterService = nodeEngine.getClusterService();
        if (!clusterService.isJoined()) {
            return new CompletedFuture<Object>(null, null, new CallerRunsExecutor());
        }

        warmUpPartitions(nodeEngine);

        RestartingMemberIterator memberIterator = new RestartingMemberIterator(clusterService, maxRetries);

        // we are going to iterate over all members and invoke an operation on each of them
        InvokeOnMemberFunction invokeOnMemberFunction = new InvokeOnMemberFunction(operationSupplier, nodeEngine,
                memberIterator);
        Iterator<ICompletableFuture<Object>> invocationIterator = map(memberIterator, invokeOnMemberFunction);

        ILogger logger = nodeEngine.getLogger(ChainingFuture.class);
        ExecutionService executionService = nodeEngine.getExecutionService();
        ManagedExecutorService executor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);

        // ChainingFuture uses the iterator to start invocations
        // it invokes on another member only when the previous invocation is completed (so invocations are serial)
        // the future itself completes only when the last invocation completes (or if there is an error)
        return new ChainingFuture<Object>(invocationIterator, executor, memberIterator, logger);
    }

    private static void warmUpPartitions(NodeEngine nodeEngine) {
        ClusterService clusterService = nodeEngine.getClusterService();
        if (!clusterService.getClusterState().isMigrationAllowed()) {
            return;
        }

        InternalPartitionService partitionService = (InternalPartitionService) nodeEngine.getPartitionService();
        if (partitionService.getMemberGroupsSize() == 0) {
            return;
        }

        for (int i = 0; i < partitionService.getPartitionCount(); i++) {
            try {
                partitionService.getPartitionOwnerOrWait(i);
            } catch (IllegalStateException e) {
                if (!clusterService.getClusterState().isMigrationAllowed()) {
                    return;
                }
                throw e;
            } catch (NoDataMemberInClusterException e) {
                return;
            }
        }
    }

    // IFunction extends Serializable, but this function is only executed locally
    @SerializableByConvention
    private static class InvokeOnMemberFunction implements IFunction<Member, ICompletableFuture<Object>> {
        private static final long serialVersionUID = 2903680336421872278L;

        private final transient Supplier<Operation> operationSupplier;
        private final transient NodeEngine nodeEngine;
        private final transient RestartingMemberIterator memberIterator;
        private final long retryDelayMillis;
        private volatile int lastRetryCount;

        InvokeOnMemberFunction(Supplier<Operation> operationSupplier, NodeEngine nodeEngine,
                RestartingMemberIterator memberIterator) {
            this.operationSupplier = operationSupplier;
            this.nodeEngine = nodeEngine;
            this.memberIterator = memberIterator;
            this.retryDelayMillis = nodeEngine.getProperties().getMillis(GroupProperty.INVOCATION_RETRY_PAUSE);
        }

        @Override
        public ICompletableFuture<Object> apply(final Member member) {
            if (isRetry()) {
                return invokeOnMemberWithDelay(member);
            }
            return invokeOnMember(member);
        }

        private boolean isRetry() {
            int currentRetryCount = memberIterator.getRetryCount();
            if (lastRetryCount == currentRetryCount) {
                return false;
            }
            lastRetryCount = currentRetryCount;
            return true;
        }

        private ICompletableFuture<Object> invokeOnMemberWithDelay(Member member) {
            SimpleCompletableFuture<Object> future = new SimpleCompletableFuture<Object>(nodeEngine);
            InvokeOnMemberTask task = new InvokeOnMemberTask(member, future);
            nodeEngine.getExecutionService().schedule(task, retryDelayMillis, TimeUnit.MILLISECONDS);
            return future;
        }

        private ICompletableFuture<Object> invokeOnMember(Member member) {
            Address address = member.getAddress();
            Operation operation = operationSupplier.get();
            String serviceName = operation.getServiceName();
            return nodeEngine.getOperationService().invokeOnTarget(serviceName, operation, address);
        }

        private class InvokeOnMemberTask implements Runnable {
            private final Member member;
            private final SimpleCompletableFuture<Object> future;

            InvokeOnMemberTask(Member member, SimpleCompletableFuture<Object> future) {
                this.member = member;
                this.future = future;
            }

            @Override
            public void run() {
                invokeOnMember(member).andThen(new ExecutionCallback<Object>() {
                    @Override
                    public void onResponse(Object response) {
                        future.setResult(response);
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.setResult(t);
                    }
                });
            }
        }
    }

    private static class SimpleCompletableFuture<T> extends AbstractCompletableFuture<T> {

        SimpleCompletableFuture(NodeEngine nodeEngine) {
            super(nodeEngine, nodeEngine.getLogger(InvocationUtil.class));
        }

        @Override
        public void setResult(Object result) {
            super.setResult(result);
        }
    }

    private static class CallerRunsExecutor implements Executor {
        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }
}
