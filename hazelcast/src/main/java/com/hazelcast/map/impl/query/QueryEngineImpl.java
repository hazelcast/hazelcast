/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.query.Target.TargetMode;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.SetUtil.allPartitionIds;
import static java.util.Collections.singletonList;

/**
 * Invokes and orchestrates the query logic returning the final result.
 * <p>
 * Knows nothing about query logic.
 * Should never invoke any query operations directly.
 * <p>
 * Should be used from top-level proxy-layer only (e.g. MapProxy, etc.).
 * <p>
 * Top level-query actors:
 * <ul>
 *     <li>QueryEngine orchestrates the queries and merging the result
 *     <li>QueryRunner -&gt; runs the query logic in the calling thread (so like evaluates the predicates and asks the index)
 * </ul>
 */
public class QueryEngineImpl implements QueryEngine {

    /**
     * Used only for tests to disable the fallback to partition operations. We
     * had issues when the fallback fixed the results in the normal case and
     * the much worsened performance went unnoticed. (E.g.
     * https://github.com/hazelcast/hazelcast/issues/18240).
     */
    public static final HazelcastProperty DISABLE_MIGRATION_FALLBACK =
            new HazelcastProperty(QueryEngineImpl.class.getName() + ".disableMigrationFallback", false);

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final QueryResultSizeLimiter queryResultSizeLimiter;
    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final ClusterService clusterService;
    private final ResultProcessorRegistry resultProcessorRegistry;
    private final boolean disableMigrationFallback;

    public QueryEngineImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.resultProcessorRegistry = mapServiceContext.getResultProcessorRegistry();
        this.disableMigrationFallback = nodeEngine.getProperties().getBoolean(DISABLE_MIGRATION_FALLBACK);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result execute(Query query, Target target) {
        return executeAsync(query, target).join();
    }

    @Override
    public CompletableFuture<Result> executeAsync(Query query, Target target) {
        Query adjustedQuery = adjustQuery(query);
        switch (target.mode()) {
            case ALL_NODES:
                adjustedQuery = Query.of(adjustedQuery).partitionIdSet(getAllPartitionIds()).build();
                return runOnGivenPartitions(adjustedQuery, adjustedQuery.getPartitionIdSet(), TargetMode.ALL_NODES);
            case LOCAL_NODE:
                adjustedQuery = Query.of(adjustedQuery).partitionIdSet(getLocalPartitionIds()).build();
                return runOnGivenPartitions(adjustedQuery, adjustedQuery.getPartitionIdSet(), TargetMode.LOCAL_NODE);
            case PARTITION_OWNER:
                int solePartition = target.partitions().solePartition();
                adjustedQuery = Query.of(adjustedQuery).partitionIdSet(target.partitions()).build();
                if (solePartition >= 0) {
                    return runOnGivenPartition(adjustedQuery, solePartition);
                } else {
                    return runOnGivenPartitions(adjustedQuery, adjustedQuery.getPartitionIdSet(), TargetMode.ALL_NODES);
                }
            default:
                throw new IllegalArgumentException("Illegal target " + target);
        }
    }
    private Query adjustQuery(Query query) {
        IterationType retrievalIterationType = getRetrievalIterationType(query.getPredicate(), query.getIterationType());
        Query adjustedQuery = Query.of(query).iterationType(retrievalIterationType).build();
        if (adjustedQuery.getPredicate() instanceof PagingPredicateImpl) {
            ((PagingPredicateImpl) adjustedQuery.getPredicate()).setIterationType(query.getIterationType());
        } else {
            if (adjustedQuery.getPredicate() == Predicates.alwaysTrue()) {
                queryResultSizeLimiter.precheckMaxResultLimitOnLocalPartitions(adjustedQuery.getMapName());
            }
        }
        return adjustedQuery;
    }

    // query thread first, fallback to partition thread
    private CompletableFuture<Result> runOnGivenPartitions(
            Query query,
            PartitionIdSet partitions,
            TargetMode targetMode) {
        CompletableFuture<Result> future = doRunOnQueryThreads(query, partitions, targetMode);
        future = future
                .thenCompose(result -> {
                    //by now, partitions updated and results accumulated
                    if (!disableMigrationFallback) {
                        if (isResultFromAnyPartitionMissing(partitions)) {
                            return doRunOnPartitionThreads(query, partitions, result);
                        }
                    }
                    return CompletableFuture.supplyAsync(() -> result);
                })
                .thenApply(result -> {
                    //by now, partitions updated once again by partitionThreads
                    assertAllPartitionsQueried(partitions);
                    return result;
                });
        return future;
    }

    // partition thread ONLY (for now)
    private CompletableFuture<Result> runOnGivenPartition(Query query, int partitionId) {
        try {
            return dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, partitionId);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private CompletableFuture<Result> doRunOnQueryThreads(
            Query query, PartitionIdSet partitionIds, TargetMode targetMode) {
        Result result = populateResult(query);
        List<CompletableFuture<Result>> futures = dispatchOnQueryThreads(query, targetMode);
        CompletableFuture<Result> future = addResultsOfPredicate(futures, result, partitionIds, false);
        return future;
    }

    private List<CompletableFuture<Result>> dispatchOnQueryThreads(Query query, TargetMode targetMode) {
        try {
            return dispatchFullQueryOnQueryThread(query, targetMode);
        } catch (Throwable t) {
            if (!(t instanceof HazelcastException)) {
                // these are programmatic errors that needs to be visible
                throw rethrow(t);
            } else if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            } else {
                // log failure to invoke query on member at fine level
                // the missing partition IDs will be queried anyway, so it's not a terminal failure
                if (logger.isFineEnabled()) {
                    logger.fine("Query invocation failed on member ", t);
                }
            }
        }
        return Collections.emptyList();
    }

    private Result populateResult(Query query) {
        return resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                queryResultSizeLimiter.getNodeResultLimit(query.getPartitionIdSet().size()));
    }

    private CompletableFuture<Result> doRunOnPartitionThreads(Query query, PartitionIdSet partitionIds, Result result) {
        try {
            List<CompletableFuture<Result>> futures = dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, partitionIds);
            CompletableFuture<Result> future = addResultsOfPredicate(futures, result, partitionIds, true);
            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    // modifies partitionIds list! Optimization not to allocate an extra collection with collected partitionIds
    private CompletableFuture<Result> addResultsOfPredicate(List<CompletableFuture<Result>> futures, Result result,
                                                            PartitionIdSet unFinishedPartitionIds, boolean rethrowAll) {
        //unFinishedPartitionIds keeps track of remaining partitions
        CompletableFuture<Result>[] futuresWithHandle = futures.stream()
                .map(future -> {
                            future = future.handle(
                                    (queryResult, t) -> {
                                        if (t != null) {
                                            if (t.getCause() instanceof QueryResultSizeExceededException
                                                    || rethrowAll) {
                                                throw rethrow(t);
                                            }
                                            logger.fine("Could not get query results", t);
                                        }
                                        return queryResult;
                                    }
                            );
                            return future;
                        }
                ).toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futuresWithHandle)
                .thenApply(v -> Arrays.stream(futuresWithHandle).map(future -> future.join()).collect(Collectors.toList()))
                .thenApply(listOfQueryResults -> {
                    for (Result queryResult : listOfQueryResults) {
                        if (queryResult == null) {
                            continue;
                        }
                        PartitionIdSet queriedPartitionIds = queryResult.getPartitionIds();
                        if (queriedPartitionIds != null) {
                            if (!unFinishedPartitionIds.containsAll(queriedPartitionIds)) {
                                // do not take into account results that contain partition IDs already removed from partitionIds
                                // collection as this means that we will count results from a single partition twice
                                // see also https://github.com/hazelcast/hazelcast/issues/6471
                                continue;
                            }
                            unFinishedPartitionIds.removeAll(queriedPartitionIds);
                            result.combine(queryResult);
                        }
                    }
                    return result;
                });
    }

    private void assertAllPartitionsQueried(PartitionIdSet mutablePartitionIds) {
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            throw new QueryException("Query aborted. Could not execute query for all partitions. Missed "
                    + mutablePartitionIds.size() + " partitions");
        }
    }

    private IterationType getRetrievalIterationType(Predicate predicate, IterationType iterationType) {
        IterationType retrievalIterationType = iterationType;
        if (predicate instanceof PagingPredicate) {
            PagingPredicate pagingPredicate = (PagingPredicate) predicate;
            if (pagingPredicate.getComparator() != null) {
                // custom comparators may act on keys and values at the same time
                retrievalIterationType = IterationType.ENTRY;
            } else {
                // in case of value, we also need to get the keys for sorting
                retrievalIterationType = iterationType == IterationType.VALUE ? IterationType.ENTRY : iterationType;
            }
        }
        return retrievalIterationType;
    }

    private PartitionIdSet getLocalPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        List<Integer> memberPartitions = partitionService.getMemberPartitions(nodeEngine.getThisAddress());
        return new PartitionIdSet(partitionCount, memberPartitions);
    }

    // package-private for tests
    PartitionIdSet getAllPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        return allPartitionIds(partitionCount);
    }

    private boolean isResultFromAnyPartitionMissing(PartitionIdSet unfinishedPartitionIds) {
        return !unfinishedPartitionIds.isEmpty();
    }

    protected QueryResultSizeLimiter getQueryResultSizeLimiter() {
        return queryResultSizeLimiter;
    }

    protected List<CompletableFuture<Result>> dispatchFullQueryOnQueryThread(Query query, TargetMode targetMode) {
        switch (targetMode) {
            case ALL_NODES:
                return dispatchFullQueryOnAllMembersOnQueryThread(query);
            case LOCAL_NODE:
                return dispatchFullQueryOnLocalMemberOnQueryThread(query);
            default:
                throw new IllegalArgumentException("Illegal target " + query);
        }
    }

    private List<CompletableFuture<Result>> dispatchFullQueryOnLocalMemberOnQueryThread(Query query) {
        Operation operation = mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
        CompletableFuture<Result> result = operationService.invokeOnTarget(
                MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
        return singletonList(result);
    }

    private List<CompletableFuture<Result>> dispatchFullQueryOnAllMembersOnQueryThread(Query query) {
        Collection<Address> members;
        if (query.getPartitionIdSet().size() == partitionService.getPartitionCount()) {
            members = clusterService.getMembers(DATA_MEMBER_SELECTOR).stream()
                    .map(m -> m.getAddress())
                    .collect(Collectors.toList());
        } else {
            members = new HashSet<>();
            for (PrimitiveIterator.OfInt iterator = query.getPartitionIdSet().intIterator(); iterator.hasNext(); ) {
                members.add(partitionService.getPartitionOwnerOrWait(iterator.next()));
            }
        }

        List<CompletableFuture<Result>> futures = new ArrayList<>(members.size());
        for (Address address : members) {
            Operation operation = createQueryOperation(query);
            CompletableFuture<Result> future = operationService.invokeOnTarget(
                    MapService.SERVICE_NAME, operation, address);
            futures.add(future);
        }
        return futures;
    }

    private Operation createQueryOperation(Query query) {
        return mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
    }

    protected List<CompletableFuture<Result>> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            Query query, PartitionIdSet partitionIds) {
        if (shouldSkipPartitionsQuery(partitionIds)) {
            return Collections.emptyList();
        }
        List<CompletableFuture<Result>> futures = new ArrayList<>(partitionIds.size());
        partitionIds.intIterator().forEachRemaining((IntConsumer) partitionId ->
                futures.add(dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(query, partitionId)));
        return futures;
    }

    protected CompletableFuture<Result> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            Query query, int partitionId) {
        Operation op = createQueryPartitionOperation(query);
        op.setPartitionId(partitionId);
        try {
            CompletableFuture<Result> future =
                    operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
            return future;
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private Operation createQueryPartitionOperation(Query query) {
        return mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryPartitionOperation(query);
    }

    private static boolean shouldSkipPartitionsQuery(PartitionIdSet partitionIds) {
        return partitionIds == null || partitionIds.isEmpty();
    }
}
