/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.internal.util.futures.ChainingFuture;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Collection;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.IterableUtil.map;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

/**
 * Utility methods for invocations.
 */
public final class InvocationUtil {
    private static final Predicate<Member> ALL_MEMBERS_MATCH = member -> true;
    private static final Predicate<Member> ALL_BUT_LOCAL_MEMBERS_MATCH = member -> !member.localMember();

    private InvocationUtil() {
    }

    /**
     * Invoke operation on all cluster members.
     *
     * <p>
     * The invocation is serial: It iterates over all members starting from the oldest member to the youngest one.
     * If there is a cluster membership change while invoking then it will restart invocations on all members. This
     * implies the operation should be idempotent.
     *
     * <p>
     * If there is an exception - other than {@link MemberLeftException},
     * {@link TargetNotMemberException} or
     * {@link HazelcastInstanceNotActiveException} while invoking then the iteration
     * is interrupted and the exception is propagated to the caller.
     * <p>
     * When invocations fail with <b>only</b> {@link ClusterTopologyChangedException}, the invocations are retried.
     * When invocations fail {@link MemberLeftException}, {@link TargetNotMemberException} or
     * {@link HazelcastInstanceNotActiveException} the exceptions are ignored.
     */
    public static <V> InternalCompletableFuture<V> invokeOnStableClusterSerial(
            NodeEngine nodeEngine,
            Supplier<? extends Operation> operationSupplier,
            int maxRetries
    ) {

        ClusterService clusterService = nodeEngine.getClusterService();
        if (!clusterService.isJoined()) {
            return newCompletedFuture(null);
        }

        RestartingMemberIterator memberIterator = new RestartingMemberIterator(clusterService, maxRetries);

        // we are going to iterate over all members and invoke an operation on each of them
        InvokeOnMemberFunction invokeOnMemberFunction = new InvokeOnMemberFunction(
                operationSupplier,
                nodeEngine,
                memberIterator
        );
        Iterator<InternalCompletableFuture<Object>> invocationIterator = map(memberIterator, invokeOnMemberFunction);

        // ChainingFuture uses the iterator to start invocations
        // it invokes on another member only when the previous invocation is completed (so invocations are serial)
        // the future itself completes only when the last invocation completes (or if there is an error)
        return new ChainingFuture(invocationIterator, memberIterator);
    }

    /**
     * Constructs a local execution with retry logic. The operation must not
     * have an {@link OperationResponseHandler}, it must return a response
     * and it must not validate the target.
     *
     * @return the local execution
     * @throws IllegalArgumentException if the operation has a response handler
     *                                  set, if it does not return a response
     *                                  or if it validates the operation target
     * @see Operation#returnsResponse()
     * @see Operation#getOperationResponseHandler()
     * @see Operation#validatesTarget()
     */
    public static LocalRetryableExecution executeLocallyWithRetry(NodeEngine nodeEngine, Operation operation) {
        if (operation.getOperationResponseHandler() != null) {
            throw new IllegalArgumentException("Operation must not have a response handler set");
        }
        if (!operation.returnsResponse()) {
            throw new IllegalArgumentException("Operation must return a response");
        }
        if (operation.validatesTarget()) {
            throw new IllegalArgumentException("Operation must not validate the target");
        }
        final LocalRetryableExecution execution = new LocalRetryableExecution(nodeEngine, operation);
        execution.run();
        return execution;
    }

    public static InternalCompletableFuture<Collection<UUID>> invokeOnStableClusterParallel(
            NodeEngine nodeEngine,
            Supplier<Operation> operationSupplier,
            int maxRetries
    ) {
        return invokeOnStableClusterParallel(nodeEngine, operationSupplier, maxRetries, ALL_MEMBERS_MATCH);
    }

    public static InternalCompletableFuture<Collection<UUID>> invokeOnStableClusterParallelExcludeLocal(
            NodeEngine nodeEngine,
            Supplier<Operation> operationSupplier,
            int maxRetries
    ) {
        return invokeOnStableClusterParallel(nodeEngine, operationSupplier, maxRetries, ALL_BUT_LOCAL_MEMBERS_MATCH);
    }

    /**
     * Invokes the given operation on all cluster members that
     * {@code memberFilter} returns {@code true} in parallel.
     *
     * <p>
     * The operation is retried until the cluster is stable between the start
     * and the end of the invocations.
     *
     * <p>
     * The operations invoked with this method should be idempotent.
     *
     * <p>
     * If one of the invocations throw any exception other than the
     * {@link ClusterTopologyChangedException}, {@link MemberLeftException},
     * {@link TargetNotMemberException}, or
     * {@link HazelcastInstanceNotActiveException} the method fails with that
     * exception. When invocations fail with <b>only</b>
     * {@code ClusterTopologyChangedException}, the invocations are retried.
     * When invocations fail with <b>only</b> {@code MemberLeftException},
     * {@code TargetNotMemberException}, or
     * {@code HazelcastInstanceNotActiveException} the exceptions are ignored
     * and the method returns the current member UUIDs, in a similar manner to
     * {@link #invokeOnStableClusterSerial(NodeEngine, Supplier, int)}.
     *
     * <p>
     * Between each retry, the parallel invocations are delayed for
     * {@link ClusterProperty#INVOCATION_RETRY_PAUSE} milliseconds.
     *
     * @return the UUID collection of the members in stable cluster
     */
    public static InternalCompletableFuture<Collection<UUID>> invokeOnStableClusterParallel(
            NodeEngine nodeEngine,
            Supplier<Operation> operationSupplier,
            int maxRetries,
            Predicate<Member> memberFilter
    ) {
        ParallelOperationInvoker invoker
                = new ParallelOperationInvoker(nodeEngine, operationSupplier, maxRetries, memberFilter);
        return invoker.invoke();
    }

    private static class InvokeOnMemberFunction implements Function<Member, InternalCompletableFuture<Object>> {
        private final transient Supplier<? extends Operation> operationSupplier;
        private final transient NodeEngine nodeEngine;
        private final transient RestartingMemberIterator memberIterator;
        private final long retryDelayMillis;
        private volatile int lastRetryCount;

        InvokeOnMemberFunction(Supplier<? extends Operation> operationSupplier,
                               NodeEngine nodeEngine,
                               RestartingMemberIterator memberIterator) {
            this.operationSupplier = operationSupplier;
            this.nodeEngine = nodeEngine;
            this.memberIterator = memberIterator;
            this.retryDelayMillis = nodeEngine.getProperties().getMillis(ClusterProperty.INVOCATION_RETRY_PAUSE);
        }

        @Override
        public InternalCompletableFuture<Object> apply(final Member member) {
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

        private InternalCompletableFuture<Object> invokeOnMemberWithDelay(Member member) {
            InternalCompletableFuture<Object> future = new InternalCompletableFuture<>();
            InvokeOnMemberTask task = new InvokeOnMemberTask(member, future);
            nodeEngine.getExecutionService().schedule(task, retryDelayMillis, TimeUnit.MILLISECONDS);
            return future;
        }

        private InternalCompletableFuture<Object> invokeOnMember(Member member) {
            Address address = member.getAddress();
            Operation operation = operationSupplier.get();
            String serviceName = operation.getServiceName();
            return nodeEngine.getOperationService().invokeOnTarget(serviceName, operation, address);
        }

        private class InvokeOnMemberTask implements Runnable {
            private final Member member;
            private final CompletableFuture<Object> future;

            InvokeOnMemberTask(Member member, CompletableFuture<Object> future) {
                this.member = member;
                this.future = future;
            }

            @Override
            public void run() {
                invokeOnMember(member).whenCompleteAsync((response, t) -> {
                    if (t == null) {
                        future.complete(response);
                    } else {
                        future.completeExceptionally(t);
                    }
                }, nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR));
            }
        }
    }
}
