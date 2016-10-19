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
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.query.MapQueryEngineUtils.addResultsOfPagingPredicate;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.addResultsOfPredicate;
import static com.hazelcast.map.impl.query.MapQueryEngineUtils.newQueryResult;
import static com.hazelcast.spi.ExecutionService.QUERY_EXECUTOR;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;
import static java.util.Collections.singletonList;

/**
 * Invokes query logic using the QueryDispatcher.
 * Knows nothing about query logic or how to dispatch query operations. It relies on query dispatcher only.
 * It never dispatches query operations directly.
 */
public class MapQueryEngineImpl implements MapQueryEngine {

    protected MapServiceContext mapServiceContext;
    protected NodeEngine nodeEngine;
    protected ILogger logger;
    protected QueryResultSizeLimiter queryResultSizeLimiter;
    protected InternalSerializationService serializationService;
    protected IPartitionService partitionService;
    protected OperationService operationService;
    protected ClusterService clusterService;
    protected LocalMapStatsProvider localMapStatsProvider;
    protected ManagedExecutorService executor;
    protected MapQueryDispatcher queryDispatcher;

    public MapQueryEngineImpl(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        this.executor = nodeEngine.getExecutionService().getExecutor(QUERY_EXECUTOR);
        this.queryDispatcher = new MapQueryDispatcher(mapServiceContext);
    }

    // invoked from proxy layer
    // query thread
    // partition thread
    @Override
    public QueryResult runQueryOnLocalPartitions(String mapName, Predicate predicate, IterationType iterationType) {
        checkNotPagingPredicate(predicate);

        List<Integer> partitionIds = getLocalPartitionIds();
        QueryResult result = newQueryResult(partitionIds.size(), iterationType, queryResultSizeLimiter);

        try {
            Future<QueryResult> future = queryDispatcher
                    .dispatchFullQueryOnLocalMemberOnQueryThread(mapName, predicate, iterationType);
            List<Future<QueryResult>> futures = singletonList(future);
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    mapName, predicate, partitionIds, iterationType);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return result;
    }

    // invoked from proxy layer
    // query thread
    // partition thread
    @Override
    public Set runQueryOnLocalPartitionsWithPagingPredicate(
            String mapName, PagingPredicate predicate, IterationType iterationType) {
        predicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        List<Integer> partitionIds = getLocalPartitionIds();

        // in case of value, we also need to get the keys for sorting.
        IterationType retrievalIterationType = iterationType == IterationType.VALUE ? IterationType.ENTRY : iterationType;

        // query the local partitions
        try {
            Future<QueryResult> future = queryDispatcher
                    .dispatchFullQueryOnLocalMemberOnQueryThread(mapName, predicate, retrievalIterationType);
            List<Future<QueryResult>> futures = singletonList(future);
            // modifies partitionIds list!
            addResultsOfPagingPredicate(serializationService, futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, predicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        // query the remaining partitions that are not local to the member
        try {
            List<Future<QueryResult>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    mapName, predicate, partitionIds, retrievalIterationType);
            addResultsOfPagingPredicate(serializationService, futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return getSortedQueryResultSet(resultList, predicate, iterationType);
    }

    // invoked from proxy layer
    // query thread
    // partition thread
    @Override
    public QueryResult runQueryOnAllPartitions(String mapName, Predicate predicate, IterationType iterationType) {
        checkNotPagingPredicate(predicate);
        if (predicate == TruePredicate.INSTANCE) {
            queryResultSizeLimiter.checkMaxResultLimitOnLocalPartitions(mapName);
        }

        Set<Integer> partitionIds = getAllPartitionIds();
        QueryResult result = newQueryResult(partitionIds.size(), iterationType, queryResultSizeLimiter);

        // query the local partitions
        try {
            List<Future<QueryResult>> futures = queryDispatcher.dispatchFullQueryOnAllMembersOnQueryThread(
                    mapName, predicate, iterationType);
            // modifies partitionIds list!
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        // query the remaining partitions that are not local to the member
        try {
            List<Future<QueryResult>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    mapName, predicate, partitionIds, iterationType);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return result;
    }

    // invoked from proxy layer
    // query thread
    // partition thread
    @Override
    public Set runQueryOnAllPartitionsWithPagingPredicate(
            String mapName, PagingPredicate predicate, IterationType iterationType) {
        predicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        Set<Integer> partitionIds = getAllPartitionIds();

        // in case of value, we also need to get the keys for sorting.
        IterationType retrievalIterationType = iterationType == IterationType.VALUE ? IterationType.ENTRY : iterationType;

        // query the local partitions
        try {
            List<Future<QueryResult>> futures = queryDispatcher
                    .dispatchFullQueryOnAllMembersOnQueryThread(mapName, predicate, retrievalIterationType);
            // modifies partitionIds list!
            addResultsOfPagingPredicate(serializationService, futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, predicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        // query the remaining partitions that are not local to the member
        try {
            List<Future<QueryResult>> futures = queryDispatcher.dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(
                    mapName, predicate, partitionIds, retrievalIterationType);
            addResultsOfPagingPredicate(serializationService, futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return getSortedQueryResultSet(resultList, predicate, iterationType);
    }

    // invoked from proxy layer
    // partition thread ONLY
    @Override
    public QueryResult runQueryOnSinglePartition(
            String mapName, Predicate predicate, IterationType iterationType, int partitionId) {
        checkNotPagingPredicate(predicate);

        try {
            Future<QueryResult> result = queryDispatcher
                    .dispatchPartitionScanQueryOnOwnerMemberOnPartitionThread(mapName, predicate, partitionId, iterationType);
            return result.get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    protected void checkNotPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Predicate should not be a paging predicate");
        }
    }

    protected List<Integer> getLocalPartitionIds() {
        return partitionService.getMemberPartitions(nodeEngine.getThisAddress());
    }

    protected Set<Integer> getAllPartitionIds() {
        int partitionCount = partitionService.getPartitionCount();
        return createSetWithPopulatedPartitionIds(partitionCount);
    }

    protected Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
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
