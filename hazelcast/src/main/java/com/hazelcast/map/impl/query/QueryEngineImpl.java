/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryException;
import com.hazelcast.query.impl.predicates.PagingPredicateImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.query.Target.ALL_NODES;
import static com.hazelcast.map.impl.query.Target.LOCAL_NODE;
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
 * <li>QueryEngine orchestrates the queries and  merging the result</li>
 * <li>QueryRunner -&gt; runs the query logic in the calling thread (so like evaluates the predicates and asks the index)</li>
 * </ul>
 */
public class QueryEngineImpl implements QueryEngine {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final QueryResultSizeLimiter queryResultSizeLimiter;
    private final IPartitionService partitionService;
    private final OperationService operationService;
    private final ClusterService clusterService;
    private final ResultProcessorRegistry resultProcessorRegistry;

    public QueryEngineImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.resultProcessorRegistry = mapServiceContext.getResultProcessorRegistry();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Result execute(Query query, Target target) {
        Query adjustedQuery = adjustQuery(query);
        switch (target.mode()) {
            case ALL_NODES:
                return runOnAllPartitions(adjustedQuery);
            case LOCAL_NODE:
                return runOnLocalPartitions(adjustedQuery);
            case PARTITION_OWNER:
                return runOnGivenPartition(adjustedQuery, target);
            default:
                throw new IllegalArgumentException("Illegal target " + query);
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
    private Result runOnLocalPartitions(Query query) {
        PartitionIdSet mutablePartitionIds = getLocalPartitionIds();

        Result result = doRunOnQueryThreads(query, mutablePartitionIds, LOCAL_NODE);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunOnPartitionThreads(query, mutablePartitionIds, result);
        }
        assertAllPartitionsQueried(mutablePartitionIds);

        return result;
    }

    // query thread first, fallback to partition thread
    private Result runOnAllPartitions(Query query) {
        PartitionIdSet mutablePartitionIds = getAllPartitionIds();

        Result result = doRunOnQueryThreads(query, mutablePartitionIds, ALL_NODES);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunOnPartitionThreads(query, mutablePartitionIds, result);
        }
        assertAllPartitionsQueried(mutablePartitionIds);

        return result;
    }

    // partition thread ONLY (for now)
    private Result runOnGivenPartition(Query query, Target target) {
        try {
            return dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, target.partitionId()).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private Result doRunOnQueryThreads(Query query, PartitionIdSet partitionIds, Target target) {
        Result result = populateResult(query, partitionIds);
        List<Future<Result>> futures = dispatchOnQueryThreads(query, target);
        addResultsOfPredicate(futures, result, partitionIds, false);
        return result;
    }

    private List<Future<Result>> dispatchOnQueryThreads(Query query, Target target) {
        try {
            return dispatchFullQueryOnQueryThread(query, target);
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

    private Result populateResult(Query query, PartitionIdSet partitionIds) {
        return resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                queryResultSizeLimiter.getNodeResultLimit(partitionIds.size()));
    }

    private void doRunOnPartitionThreads(Query query, PartitionIdSet partitionIds, Result result) {
        try {
            List<Future<Result>> futures = dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, partitionIds);
            addResultsOfPredicate(futures, result, partitionIds, true);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    // modifies partitionIds list! Optimization not to allocate an extra collection with collected partitionIds
    private void addResultsOfPredicate(List<Future<Result>> futures, Result result,
                                       PartitionIdSet finishedPartitionIds, boolean rethrowAll) {
        for (Future<Result> future : futures) {
            Result queryResult = null;

            try {
                queryResult = future.get();
            } catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException || rethrowAll) {
                    throw rethrow(t);
                }
                logger.fine("Could not get query results", t);
            }

            if (queryResult == null) {
                continue;
            }
            PartitionIdSet queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                if (!finishedPartitionIds.containsAll(queriedPartitionIds)) {
                    // do not take into account results that contain partition IDs already removed from partitionIds
                    // collection as this means that we will count results from a single partition twice
                    // see also https://github.com/hazelcast/hazelcast/issues/6471
                    continue;
                }
                finishedPartitionIds.removeAll(queriedPartitionIds);
                result.combine(queryResult);
            }
        }
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

    private PartitionIdSet getAllPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        return allPartitionIds(partitionCount);
    }

    private boolean isResultFromAnyPartitionMissing(PartitionIdSet finishedPartitionIds) {
        return !finishedPartitionIds.isEmpty();
    }

    protected QueryResultSizeLimiter getQueryResultSizeLimiter() {
        return queryResultSizeLimiter;
    }

    protected List<Future<Result>> dispatchFullQueryOnQueryThread(Query query, Target target) {
        switch (target.mode()) {
            case ALL_NODES:
                return dispatchFullQueryOnAllMembersOnQueryThread(query);
            case LOCAL_NODE:
                return dispatchFullQueryOnLocalMemberOnQueryThread(query);
            default:
                throw new IllegalArgumentException("Illegal target " + query);
        }
    }

    private List<Future<Result>> dispatchFullQueryOnLocalMemberOnQueryThread(Query query) {
        Operation operation = mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
        Future<Result> result = operationService.invokeOnTarget(
                MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
        return singletonList(result);
    }

    private List<Future<Result>> dispatchFullQueryOnAllMembersOnQueryThread(Query query) {
        Collection<Member> members = clusterService.getMembers(DATA_MEMBER_SELECTOR);
        List<Future<Result>> futures = new ArrayList<>(members.size());
        for (Member member : members) {
            Operation operation = createQueryOperation(query);
            Future<Result> future = operationService.invokeOnTarget(
                    MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    private Operation createQueryOperation(Query query) {
        return mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
    }

    protected List<Future<Result>> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
            Query query, PartitionIdSet partitionIds) {
        if (shouldSkipPartitionsQuery(partitionIds)) {
            return Collections.emptyList();
        }
        List<Future<Result>> futures = new ArrayList<>(partitionIds.size());
        partitionIds.intIterator().forEachRemaining((IntConsumer) partitionId ->
                futures.add(dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(query, partitionId)));
        return futures;
    }

    protected Future<Result> dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(Query query, int partitionId) {
        Operation op = createQueryPartitionOperation(query);
        op.setPartitionId(partitionId);
        try {
            return operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
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
