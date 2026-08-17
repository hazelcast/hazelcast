/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.query;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.SilentException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.vector.SearchOptions;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.vector.VectorValues;
import com.hazelcast.vector.internal.impl.VectorCollectionService;
import com.hazelcast.vector.internal.impl.ops.SearchMemberOperation;
import com.hazelcast.vector.internal.impl.ops.SearchMemberResult;
import com.hazelcast.vector.internal.impl.ops.SearchOperation;
import com.hazelcast.vector.internal.impl.storage.AbstractVectorIndex;
import io.github.jbellis.jvector.annotations.VisibleForTesting;
import io.github.jbellis.jvector.util.GrowableLongHeap;

import javax.annotation.Nullable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;

import static com.hazelcast.internal.util.ConcurrencyUtil.CALLER_RUNS;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static com.hazelcast.internal.util.JVMUtil.OBJECT_HEADER_SIZE;
import static com.hazelcast.internal.util.JVMUtil.REFERENCE_COST_IN_BYTES;

/**
 * Two stage aggregation. First each member searches partitions it owns (determined by the coordinator member)
 * and sends partially aggregated results to the coordinator which does any necessary retries (e.g. due to migrations)
 * and performs final results aggregation.
 */
public class TwoStageSearcher implements Searcher {
    public static final long FIXED_HEAP_BYTES_USED = OBJECT_HEADER_SIZE + 4L * REFERENCE_COST_IN_BYTES;

    private final IPartitionService partitionService;
    private final OperationInvoker operationInvoker;
    private final ILogger logger;
    private final Supplier<Boolean> activityStatusSupplier;

    public TwoStageSearcher(NodeEngine engine) {
        partitionService = engine.getPartitionService();
        operationInvoker = new OperationInvoker() {
            @Override
            public <E> CompletableFuture<E> invokeOnTargetAsync(Operation op, Address target) {
                return engine.getOperationService().invokeOnTargetAsync(VectorCollectionService.SERVICE_NAME, op, target);
            }

            @Override
            public <E> CompletableFuture<E> invokeOnPartitionAsync(Operation op, int partition) {
                return engine.getOperationService().invokeOnPartitionAsync(VectorCollectionService.SERVICE_NAME, op, partition);
            }
        };
        activityStatusSupplier = () -> engine.getNode().getState() != NodeState.SHUT_DOWN;
        logger = engine.getLogger(AbstractVectorIndex.class);
    }

    @VisibleForTesting
    interface OperationInvoker {
        <E> CompletableFuture<E> invokeOnTargetAsync(Operation op, Address target);
        <E> CompletableFuture<E> invokeOnPartitionAsync(Operation op, int partition);
    }

    // used for testing
    private TwoStageSearcher(IPartitionService partitionService, OperationInvoker operationInvoker, ILogger logger) {
        this.partitionService = partitionService;
        this.operationInvoker = operationInvoker;
        this.logger = logger;
        this.activityStatusSupplier = () -> true;
    }

    @Override
    public CompletableFuture<SearchResults<Data, Data>> search(String collectionName, VectorValues vectors, SearchOptions options,
                                                         @Nullable ClientEndpoint endpoint) {
        int partitionCount = getPartitionCount();
        var partitionsMap = partitionService.getMemberPartitionsMap();

        int membersCount = partitionsMap.size();
        // in worst case all members will say that all partitions have been migrated or all members left the cluster
        // (probably have been replaced by other members) but usually we might only need none or a few more than number of members
        // (assuming 1 extra place initially).
        //
        // In member invocations stage partitionId of first partition is used as index of the result.
        // In partition-retry stage sole partitionId is used - there should be no overlap.
        QueryResult clusterResult = new QueryResult(partitionCount, membersCount + 1,
                options.getLimit(),
                GrowableLongHeap::new);

        @SuppressWarnings("unchecked")
        CompletableFuture<PartitionIdSet>[] memberFutures = partitionsMap.entrySet().stream().map(entry -> {
            var member = entry.getKey();
            var partitions = new PartitionIdSet(partitionCount, entry.getValue());
            CompletableFuture<SearchMemberResult> future = operationInvoker.invokeOnTargetAsync(
                    new SearchMemberOperation(collectionName, vectors, options, partitions)
                            .setCallerUuid(endpoint != null ? endpoint.getUuid() : null),
                    member);
            return future.handleAsync((r, e) -> {
                if (e == null) {
                    var firstPartitionId = r.scannedPartitions().firstPartition();
                    // result without partitions should not be added,
                    // as all requested partitions will be retried
                    if (firstPartitionId >= 0) {
                        clusterResult.addResult(firstPartitionId, r.results());
                    }
                    return r.scannedPartitions();
                } else {
                    if (isRoutineOperationException(e) || e instanceof SilentException) {
                        if (logger.isFineEnabled()) {
                            logger.fine("Member search failed for partitions " + partitions + " on member " + member, e);
                        }
                    } else {
                        logger.warning("Member search failed for partitions " + partitions + " on member " + member, e);
                    }
                    // if entire member search failed (possibly member crashed or terminated),
                    // we will retry search on each partition individually
                    return new PartitionIdSet(0);
                }
            }, CALLER_RUNS);
        }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(memberFutures)
                .thenComposeAsync(v -> retryPartitions(memberFutures, clusterResult,
                                collectionName, vectors, options, endpoint), CALLER_RUNS)
                .thenApplyAsync(v -> clusterResult.complete(), CALLER_RUNS);
    }

    private int getPartitionCount() {
        return partitionService.getPartitionCount();
    }

    private CompletionStage<Void> retryPartitions(CompletableFuture<PartitionIdSet>[] memberFutures,
                                                  QueryResult clusterResult,
                                                  String collectionName, VectorValues vectors, SearchOptions options,
                                                  @Nullable ClientEndpoint endpoint) {
        PartitionIdSet finishedPartitions = new PartitionIdSet(getPartitionCount());
        for (CompletableFuture<PartitionIdSet> f : memberFutures) {
            // PartitionIdSet is not thread safe, so do the aggregation in single thread
            finishedPartitions.addAll(f.getNow(null));
        }
        if (!finishedPartitions.isMissingPartitions()) {
            return CompletableFuture.completedFuture(null);
        }
        if (!activityStatusSupplier.get()) {
            // The instance was shut down, ditch the query without verbose logs
            // Partition retries would fail anyway.
            logger.fine("Not retrying partitions, instance was shut down");
            return CompletableFuture.failedFuture(new HazelcastInstanceNotActiveException());
        }
        PartitionIdSet missingPartitions = new PartitionIdSet(finishedPartitions);
        missingPartitions.complement();

        logger.fine("Retrying partitions %s", missingPartitions);

        @SuppressWarnings("rawtypes")
        CompletableFuture[] retryFutures = missingPartitions.stream().map(partitionId -> {
            // Retry partitions individually.
            // We might consider using invokeOnPartitionsAsync if there are many partitions to retry.
            //
            // Search operation generally should not fail due to concurrent migrations, even when offloaded,
            // and if it needs to be retried due to PartitionMigratingException, it will be handled
            // transparently by the Invocation.
            CompletableFuture<SearchResults<Data, Data>> future = operationInvoker.invokeOnPartitionAsync(
                            new SearchOperation(collectionName, vectors, options)
                                    .setCallerUuid(endpoint != null ? endpoint.getUuid() : null),
                            partitionId);
            return future.handleAsync((r, t) -> {
                if (t == null) {
                    clusterResult.addResult(partitionId, r);
                    return null;
                } else {
                    // in case of error at this stage entire query fails and may be retried by client
                    if (isRoutineOperationException(t) || t instanceof SilentException) {
                        if (logger.isFineEnabled()) {
                            logger.fine("Query failed during partition retry on partitionId=" + partitionId, t);
                        }
                    } else {
                        logger.warning("Query failed during partition retry on partitionId=" + partitionId, t);
                    }
                    throw sneakyThrow(t);
                }
            }, CALLER_RUNS);
        }).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(retryFutures);
    }

    /**
     * Recognizes exceptions that can be routinely thrown during search
     * in some normal situations like topology changes.
     * They cause appropriate retry and do not have to be logged.
     */
    private static boolean isRoutineOperationException(Throwable e) {
        return e instanceof HazelcastInstanceNotActiveException
                || e instanceof MemberLeftException
                || e instanceof TargetNotMemberException;
    }

    @Override
    public long heapBytesUsed() {
        return FIXED_HEAP_BYTES_USED;
    }
}
