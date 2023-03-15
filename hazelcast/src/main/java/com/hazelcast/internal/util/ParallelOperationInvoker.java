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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.ClusterTopologyChangedException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.InternalCompletableFuture;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;


/**
 * See {@link InvocationUtil#invokeOnStableClusterParallel(NodeEngine, Supplier, int, Predicate)}.
 */
class ParallelOperationInvoker {
    private final NodeEngine nodeEngine;
    private final ClusterService clusterService;
    private final Supplier<Operation> operationSupplier;
    private final int maxRetries;
    private final long retryDelayMillis;
    private final AtomicInteger retryCount = new AtomicInteger(0);
    private final InternalCompletableFuture<Collection<UUID>> future = new InternalCompletableFuture<>();
    private final Predicate<Member> memberFilter;
    private volatile Set<Member> members;

    ParallelOperationInvoker(NodeEngine nodeEngine,
                             Supplier<Operation> operationSupplier,
                             int maxRetries,
                             Predicate<Member> memberFilter) {
        this.nodeEngine = nodeEngine;
        this.clusterService = nodeEngine.getClusterService();
        this.operationSupplier = operationSupplier;
        this.maxRetries = maxRetries;
        this.retryDelayMillis = nodeEngine.getProperties().getMillis(ClusterProperty.INVOCATION_RETRY_PAUSE);
        this.memberFilter = memberFilter;
    }

    public InternalCompletableFuture<Collection<UUID>> invoke() {
        doInvoke();
        return future;
    }

    private void doInvoke() {
        members = clusterService.getMembers();
        InternalCompletableFuture[] futures = invokeOnMatchingMembers(members);
        CompletableFuture.allOf(futures)
                .whenCompleteAsync(
                        (ignored, throwable) -> completeFutureOrRetry(throwable == null, futures),
                        ConcurrencyUtil.getDefaultAsyncExecutor());
    }

    private void doInvokeWithDelay() {
        nodeEngine.getExecutionService().schedule(this::doInvoke, retryDelayMillis, TimeUnit.MILLISECONDS);
    }

    private InternalCompletableFuture[] invokeOnMatchingMembers(Collection<Member> members) {
        return members.stream()
                .filter(memberFilter)
                .map(this::invokeOnMember)
                .toArray(InternalCompletableFuture[]::new);
    }

    private InternalCompletableFuture<Void> invokeOnMember(Member member) {
        Operation operation = operationSupplier.get();
        String serviceName = operation.getServiceName();
        Address target = member.getAddress();
        return nodeEngine.getOperationService().invokeOnTargetAsync(serviceName, operation, target);
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

    private boolean isIgnorableException(Throwable t) {
        // Same as the ignored exceptions in the
        // RestartingMemberIterator#handle
        return t instanceof MemberLeftException
                || t instanceof TargetNotMemberException
                || t instanceof HazelcastInstanceNotActiveException;
    }

    private void onExceptionalCompletion(InternalCompletableFuture[] futures, Collection<Member> currentMembers) {
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
                } else if (!isIgnorableException(cause)) {
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

        if (!currentMembers.equals(members)) {
            // It may be possible that we get an ignorable exception but the topology is changed.
            isClusterTopologyChanged = true;
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
