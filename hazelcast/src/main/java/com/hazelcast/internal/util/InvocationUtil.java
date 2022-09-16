/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.internal.util.futures.ChainingFuture;
import com.hazelcast.internal.util.iterator.RestartingMemberIterator;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.hazelcast.internal.util.IterableUtil.map;
import static com.hazelcast.spi.impl.InternalCompletableFuture.newCompletedFuture;

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

    /**
     * Invokes the given operation on all cluster members (excluding this
     * member), in parallel.
     * <p>
     * The operation is retried until the cluster is stable between the start
     * and the end of the invocations.
     * <p>
     * The operations invoked with this method should be idempotent.
     * <p>
     * If one of the invocations throw any exception other than the
     * {@link ClusterTopologyChangedException}, {@link MemberLeftException},
     * {@link TargetNotMemberException}, or
     * {@link HazelcastInstanceNotActiveException} the method fails with that
     * exception. When invocations fail with <b>only</b>
     * {@code ClusterTopologyChangedException}, the invocations are retried.
     * When invocations fail with <b>only<b/> {@code MemberLeftException},
     * {@code TargetNotMemberException}, or
     * {@code HazelcastInstanceNotActiveException} the exceptions are ignored
     * and the method returns the current member UUIDs, in a similar manner to
     * {@link #invokeOnStableClusterSerial(NodeEngine, Supplier, int)}.
     * <p>
     * Between each retry, the parallel invocations are delayed for
     * {@link ClusterProperty#INVOCATION_RETRY_PAUSE} milliseconds.
     *
     * @return the collection of the member UUIDs that the operations are
     * invoked on
     */
    public static InternalCompletableFuture<Collection<UUID>> invokeOnStableClusterParallel(
            NodeEngine nodeEngine,
            Supplier<Operation> operationSupplier,
            int maxRetries
    ) {
        ParallelOperationInvoker invoker = new ParallelOperationInvoker(nodeEngine, operationSupplier, maxRetries);
        return invoker.invoke();
    }

    private static class InvokeOnMemberFunction implements Function<Member, InternalCompletableFuture<Object>> {
        private final transient Supplier<? extends Operation> operationSupplier;
        private final transient NodeEngine nodeEngine;
        private final transient RestartingMemberIterator memberIterator;
        private final long retryDelayMillis;
        private volatile int lastRetryCount;

        InvokeOnMemberFunction(Supplier<? extends Operation> operationSupplier, NodeEngine nodeEngine,
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
                });
            }
        }
    }

    private static final class ParallelOperationInvoker {
        private final NodeEngine nodeEngine;
        private final ClusterService clusterService;
        private final Supplier<Operation> operationSupplier;
        private final int maxRetries;
        private final long retryDelayMillis;
        private final AtomicInteger retryCount = new AtomicInteger(0);
        private final InternalCompletableFuture<Collection<UUID>> future = new InternalCompletableFuture<>();
        private volatile Set<Member> members;

        ParallelOperationInvoker(NodeEngine nodeEngine, Supplier<Operation> operationSupplier, int maxRetries) {
            this.nodeEngine = nodeEngine;
            this.clusterService = nodeEngine.getClusterService();
            this.operationSupplier = operationSupplier;
            this.maxRetries = maxRetries;
            this.retryDelayMillis = nodeEngine.getProperties().getMillis(ClusterProperty.INVOCATION_RETRY_PAUSE);
        }

        public InternalCompletableFuture<Collection<UUID>> invoke() {
            doInvoke();
            return future;
        }

        private void doInvoke() {
            members = clusterService.getMembers();
            InternalCompletableFuture[] futures = invokeOnAllMembersExcludingThis(members);
            CompletableFuture.allOf(futures)
                    .whenCompleteAsync(
                            (ignored, throwable) -> completeFutureOrRetry(throwable == null, futures),
                            ConcurrencyUtil.getDefaultAsyncExecutor());
        }

        private void doInvokeWithDelay() {
            nodeEngine.getExecutionService().schedule(this::doInvoke, retryDelayMillis, TimeUnit.MILLISECONDS);
        }

        private InternalCompletableFuture[] invokeOnAllMembersExcludingThis(Collection<Member> members) {
            return members.stream()
                    .filter(member -> !nodeEngine.getThisAddress().equals(member.getAddress()))
                    .map(this::invokeOnMember)
                    .toArray(InternalCompletableFuture[]::new);
        }

        private InternalCompletableFuture<Void> invokeOnMember(Member member) {
            Operation operation = operationSupplier.get();
            String serviceName = operation.getServiceName();
            Address target = member.getAddress();
            return nodeEngine.getOperationService().invokeOnTarget(serviceName, operation, target);
        }

        private void completeFutureOrRetry(boolean noExceptionReceived, InternalCompletableFuture[] futures) {
            Set<Member> currentMembers = clusterService.getMembers();
            if (noExceptionReceived) {
                // All futures completed successfully.
                // If the member list at the start and now are equal to each
                // other, we will complete the future. If not, we will retry
                if (currentMembers.equals(members)) {
                    List<UUID> memberUuids = convertMemberListToMemberUuidList(currentMembers);
                    future.complete(memberUuids);
                } else {
                    retry();
                }
            } else {
                // At least one of the futures completed exceptionally
                onExceptionalCompletion(futures, currentMembers);
            }
        }

        private void retry() {
            if (retryCount.incrementAndGet() > maxRetries) {
                Throwable t = new HazelcastException("Cluster topology was not stable for " + maxRetries + " retries");
                future.completeExceptionally(t);
                return;
            }
            doInvokeWithDelay();
        }

        private List<UUID> convertMemberListToMemberUuidList(Collection<Member> members) {
            return members.stream()
                    .map(Member::getUuid)
                    .collect(Collectors.toList());
        }

        private boolean isExceptionCanBeIgnored(Throwable t) {
            // Same as the ignored exceptions in the
            // RestartingMemberIterator#handle
            return t instanceof MemberLeftException
                    || t instanceof TargetNotMemberException
                    || t instanceof HazelcastInstanceNotActiveException;
        }

        private void onExceptionalCompletion(InternalCompletableFuture[] futures, Collection<Member> members) {
            // We know all futures are done, and at least one of them completed
            // with an exception. We will inspect all the futures, and act
            // according to thrown exception(s)
            boolean isTerminalExceptionReceived = false;
            Throwable terminalException = null;

            boolean isClusterTopologyChanged = false;

            for (InternalCompletableFuture future : futures) {
                assert future.isDone();

                if (!future.isCompletedExceptionally()) {
                    continue;
                }

                try {
                    future.join();
                } catch (CancellationException cancellationException) {
                    // We cannot retry or ignore cancellation exception
                    isTerminalExceptionReceived = true;
                    terminalException = cancellationException;
                    break;
                } catch (CompletionException completionException) {
                    // Exceptions are wrapped inside the completion exception
                    Throwable cause = completionException.getCause();

                    if (cause instanceof ClusterTopologyChangedException) {
                        // This is not a terminal exception, and we would retry
                        // the invocations if this was the only exception thrown
                        isClusterTopologyChanged = true;
                        // Not breaking, we should inspect other futures as well
                    } else if (!isExceptionCanBeIgnored(cause)) {
                        // Some exceptions are ignored. if the cause was one of
                        // them we will continue inspecting other futures for
                        // other failures. if it was not one of the exception
                        // types that can be ignored, then it is terminal.
                        isTerminalExceptionReceived = true;
                        terminalException = cause != null ? cause : completionException;
                        break;
                    }
                } catch (Throwable t) {
                    // No other type of exception is expected as per the docs
                    // of .join(), but no harm is done by being a bit defensive
                    // here, and consider any other exception as a terminal one
                    isTerminalExceptionReceived = true;
                    terminalException = t;
                    break;
                }
            }

            if (isTerminalExceptionReceived) {
                // If any of the futures are completed with a terminal exception,
                // the others are not really important. The whole invocation should
                // fail with that terminal exception
                future.completeExceptionally(terminalException);
            } else if (isClusterTopologyChanged) {
                // If there were no terminal exception, but the cluster topology
                // is changed, we should retry
                retry();
            } else {
                // If the types of the exceptions can be ignored, we will
                // assume the invocations are completed, similar to
                // invokeOnStableClusterSerial
                List<UUID> memberUuids = convertMemberListToMemberUuidList(members);
                future.complete(memberUuids);
            }
        }
    }
}
