/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.query.MapQueryDispatcher.DispatchTarget;
import static com.hazelcast.map.impl.query.MapQueryDispatcher.DispatchTarget.ALL_MEMBERS;
import static com.hazelcast.map.impl.query.MapQueryDispatcher.DispatchTarget.LOCAL_MEMBER;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.createSetWithPopulatedPartitionIds;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.newQueryResult;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;

/**
 * Invokes and orchestrates the query logic returning the final result.
 * <p>
 * Knows nothing about query logic or how to dispatch query operations. It relies on QueryDispatcher only.
 * Should never invoke any query operations directly.
 * <p>
 * Should be used from top-level proxy-layer only (e.g. MapProxy, etc.).
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
    protected final MapQueryDispatcher queryDispatcher;

    public MapQueryEngineImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.queryDispatcher = new MapQueryDispatcher(mapServiceContext);
    }

    // query thread first, fallback to partition thread
    @Override
    public Set runQueryOnLocalPartitions(String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult) {
        Collection<Integer> mutablePartitionIds = getLocalPartitionIds();

        QueryResult result = doRunQueryOnQueryThreads(mapName, predicate, iterationType, mutablePartitionIds, LOCAL_MEMBER);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunQueryOnPartitionThreads(mapName, predicate, iterationType, mutablePartitionIds, result);
        }

        return transformResult(predicate, result, iterationType, uniqueResult);
    }

    // query thread first, fallback to partition thread
    @Override
    public Set runQueryOnAllPartitions(
            String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult) {
        Collection<Integer> mutablePartitionIds = getAllPartitionIds();

        QueryResult result = doRunQueryOnQueryThreads(
                mapName, predicate, iterationType, mutablePartitionIds, ALL_MEMBERS);
        if (isResultFromAnyPartitionMissing(mutablePartitionIds)) {
            doRunQueryOnPartitionThreads(mapName, predicate, iterationType, mutablePartitionIds, result);
        }

        return transformResult(predicate, result, iterationType, uniqueResult);
    }

    // partition thread ONLY (for now)
    @Override
    public Set runQueryOnGivenPartition(
            String mapName, Predicate predicate, IterationType iterationType, boolean uniqueResult, int partitionId) {
        try {
            Future<QueryResult> result = queryDispatcher
                    .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(mapName, predicate, partitionId, iterationType);
            return transformResult(predicate, result.get(), iterationType, uniqueResult);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private QueryResult doRunQueryOnQueryThreads(String mapName, Predicate predicate, IterationType iterationType,
                                                 Collection<Integer> partitionIds, DispatchTarget target) {
        IterationType retrievalIterationType = getRetrievalIterationType(predicate, iterationType);
        if (predicate instanceof PagingPredicate) {
            ((PagingPredicate) predicate).setIterationType(iterationType);
        } else {
            checkQueryResultLimiter(mapName, predicate);
        }
        QueryResult result = newQueryResult(partitionIds.size(), retrievalIterationType, queryResultSizeLimiter);
        dispatchQueryOnQueryThreads(mapName, predicate, partitionIds, target, retrievalIterationType, result);
        return result;
    }

    private void dispatchQueryOnQueryThreads(String mapName, Predicate predicate, Collection<Integer> partitionIds,
                                             DispatchTarget target, IterationType retrievalIterationType, QueryResult result) {
        try {
            List<Future<QueryResult>> futures = queryDispatcher
                    .dispatchFullQueryOnQueryThread(mapName, predicate, retrievalIterationType, target);

            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }
    }

    private void doRunQueryOnPartitionThreads(String mapName, Predicate predicate, IterationType iterationType,
                                              Collection<Integer> partitionIds, QueryResult result) {
        try {
            IterationType retrievalIterationType = getRetrievalIterationType(predicate, iterationType);
            List<Future<QueryResult>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    mapName, predicate, partitionIds, retrievalIterationType);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    @SuppressWarnings("unchecked")
    // modifies partitionIds list! Optimization not to allocate an extra collection with collected partitionIds
    private void addResultsOfPredicate(List<Future<QueryResult>> futures, QueryResult result,
                                       Collection<Integer> partitionIds) throws ExecutionException, InterruptedException {
        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
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
                result.addAllRows(queryResult.getRows());
            }
        }
    }

    private Set transformResult(Predicate predicate, QueryResult queryResult, IterationType iterationType, boolean unique) {
        if (predicate instanceof PagingPredicate) {
            Set result = new QueryResultCollection(serializationService, IterationType.ENTRY, false, unique, queryResult);
            return getSortedQueryResultSet(new ArrayList(result), (PagingPredicate) predicate, iterationType);
        } else {
            return new QueryResultCollection(serializationService, iterationType, false, unique, queryResult);
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

    private void checkQueryResultLimiter(String mapName, Predicate predicate) {
        if (predicate == TruePredicate.INSTANCE) {
            queryResultSizeLimiter.checkMaxResultLimitOnLocalPartitions(mapName);
        }
    }

    protected QueryResultSizeLimiter getQueryResultSizeLimiter() {
        return queryResultSizeLimiter;
    }
}
