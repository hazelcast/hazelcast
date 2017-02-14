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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;

/**
 * Invokes and orchestrates the query logic returning the final result.
 * <p>
 * Knows nothing about query logic or how to dispatch query operations. It relies on QueryDispatcher only.
 * Should never invoke any query operations directly.
 * <p>
 * Should be used from top-level proxy-layer only (e.g. MapProxy, etc.).
 * <p>
 * Top level-query actors:
 * - QueryEngine orchestrates the queries by dispatching query operations using QueryDispatcher and merging the result
 * - QueryDispatcher invokes query operations on the given members and partitions
 * - QueryRunner -> runs the query logic in the calling thread (so like evaluates the predicates and asks the index)
 *
 * TODO: Use bitset instead of collection for partitionIds
 */
public class MapQueryEngineImpl implements MapQueryEngine {

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final QueryResultSizeLimiter queryResultSizeLimiter;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final QueryDispatcher queryDispatcher;
    protected final ResultProcessorRegistry resultProcessorRegistry;

    public MapQueryEngineImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.queryDispatcher = new QueryDispatcher(mapServiceContext);
        this.resultProcessorRegistry = mapServiceContext.getResultProcessorRegistry();
    }

    @Override
    public Result execute(Query query, Target target) {
        Query adjustedQuery = adjustQuery(query);
        if (target.isTargetAllNodes()) {
            return runQueryOnAllPartitions(adjustedQuery);
        } else if (target.isTargetLocalNode()) {
            return runQueryOnLocalPartitions(adjustedQuery);
        } else if (target.isTargetPartitionOwner()) {
            // we do not adjust the query here - it's a single partition operation only
            return runQueryOnGivenPartition(query, target);
        }
        throw new IllegalArgumentException("Illegal target " + query);
    }

    private Query adjustQuery(Query query) {
        IterationType retrievalIterationType = getRetrievalIterationType(query.getPredicate(), query.getIterationType());
        Query adjustedQuery = Query.of(query).iterationType(retrievalIterationType).build();
        if (adjustedQuery.getPredicate() instanceof PagingPredicate) {
            ((PagingPredicate) adjustedQuery.getPredicate()).setIterationType(query.getIterationType());
        } else {
            if (adjustedQuery.getPredicate() == TruePredicate.INSTANCE) {
                queryResultSizeLimiter.precheckMaxResultLimitOnLocalPartitions(adjustedQuery.getMapName());
            }
        }
        return adjustedQuery;
    }

    // query thread first, fallback to partition thread
    private Result runQueryOnLocalPartitions(Query query) {
        Collection<Integer> mutablePartitionIds = getLocalPartitionIds();

        Result result = doRunQueryOnQueryThreads(query, mutablePartitionIds, Target.LOCAL_NODE);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunQueryOnPartitionThreads(query, mutablePartitionIds, result);
        }

        return result;
    }

    // query thread first, fallback to partition thread
    private Result runQueryOnAllPartitions(Query query) {
        Collection<Integer> mutablePartitionIds = getAllPartitionIds();

        Result result = doRunQueryOnQueryThreads(query, mutablePartitionIds, Target.ALL_NODES);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunQueryOnPartitionThreads(query, mutablePartitionIds, result);
        }

        return result;
    }

    // partition thread ONLY (for now)
    private Result runQueryOnGivenPartition(Query query, Target target) {
        try {
            return queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, target.getPartitionId()).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private Result doRunQueryOnQueryThreads(Query query, Collection<Integer> partitionIds, Target target) {
        Result result = resultProcessorRegistry.get(query.getResultType()).populateResult(query,
                queryResultSizeLimiter.getNodeResultLimit(partitionIds.size()));
        dispatchQueryOnQueryThreads(query, target, partitionIds, result);
        return result;
    }

    private void dispatchQueryOnQueryThreads(Query query, Target target, Collection<Integer> partitionIds, Result result) {
        try {
            List<Future<Result>> futures = queryDispatcher.dispatchFullQueryOnQueryThread(query, target);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.fine("Could not get results", t);
        }
    }

    private void doRunQueryOnPartitionThreads(Query query, Collection<Integer> partitionIds, Result result) {
        try {
            List<Future<Result>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    query, partitionIds);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    // modifies partitionIds list! Optimization not to allocate an extra collection with collected partitionIds
    private void addResultsOfPredicate(List<Future<Result>> futures, Result result,
                                       Collection<Integer> partitionIds) throws ExecutionException, InterruptedException {
        for (Future<Result> future : futures) {
            Result queryResult = future.get();
            if (queryResult == null) {
                continue;
            }
            Collection<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                if (!partitionIds.containsAll(queriedPartitionIds)) {
                    // do not take into account results that contain partition IDs already removed from partitionIds
                    // collection as this means that we will count results from a single partition twice
                    // see also https://github.com/hazelcast/hazelcast/issues/6471
                    continue;
                }
                partitionIds.removeAll(queriedPartitionIds);
                result.combine(queryResult);
            }
        }
    }

    private IterationType getRetrievalIterationType(Predicate predicate, IterationType iterationType) {
        IterationType retrievalIterationType = iterationType;
        if (predicate instanceof PagingPredicate) {
            // in case of value, we also need to get the keys for sorting.
            retrievalIterationType = (iterationType == IterationType.VALUE) ? IterationType.ENTRY : iterationType;
        }
        return retrievalIterationType;
    }

    private List<Integer> getLocalPartitionIds() {
        return partitionService.getMemberPartitions(nodeEngine.getThisAddress());
    }

    private Set<Integer> getAllPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        return createSetWithPopulatedPartitionIds(partitionCount);
    }

    private boolean isResultFromAnyPartitionMissing(Collection<Integer> partitionIds) {
        return !partitionIds.isEmpty();
    }

    private static Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
        Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }

    protected QueryResultSizeLimiter getQueryResultSizeLimiter() {
        return queryResultSizeLimiter;
    }
}
