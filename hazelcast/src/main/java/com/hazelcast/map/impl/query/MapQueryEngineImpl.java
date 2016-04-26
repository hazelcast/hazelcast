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

import com.hazelcast.config.CacheDeserializedValues;
import com.hazelcast.core.Member;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.LocalMapStatsProvider;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.record.Records;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.CachedQueryEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.spi.ExecutionService.QUERY_EXECUTOR;
import static com.hazelcast.spi.properties.GroupProperty.QUERY_PREDICATE_PARALLEL_EVALUATION;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.util.SortingUtil.compareAnchor;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;
import static com.hazelcast.util.SortingUtil.getSortedSubList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * The {@link MapQueryEngine} implementation.
 */
public class MapQueryEngineImpl implements MapQueryEngine {

    protected static final long QUERY_EXECUTION_TIMEOUT_MINUTES = 5;

    protected final MapServiceContext mapServiceContext;
    protected final NodeEngine nodeEngine;
    protected final ILogger logger;
    protected final QueryResultSizeLimiter queryResultSizeLimiter;
    protected final InternalSerializationService serializationService;
    protected final IPartitionService partitionService;
    protected final QueryOptimizer queryOptimizer;
    protected final OperationService operationService;
    protected final ClusterService clusterService;
    protected final LocalMapStatsProvider localMapStatsProvider;
    protected final boolean parallelEvaluation;
    protected final ManagedExecutorService executor;

    public MapQueryEngineImpl(MapServiceContext mapServiceContext, QueryOptimizer optimizer) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        this.partitionService = nodeEngine.getPartitionService();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
        this.queryOptimizer = optimizer;
        this.operationService = nodeEngine.getOperationService();
        this.clusterService = nodeEngine.getClusterService();
        this.localMapStatsProvider = mapServiceContext.getLocalMapStatsProvider();
        this.parallelEvaluation = nodeEngine.getProperties().getBoolean(QUERY_PREDICATE_PARALLEL_EVALUATION);
        this.executor = nodeEngine.getExecutionService().getExecutor(QUERY_EXECUTOR);
    }

    QueryResultSizeLimiter getQueryResultSizeLimiter() {
        return queryResultSizeLimiter;
    }

    @Override
    public QueryResult queryLocalPartitions(String mapName, Predicate predicate, IterationType iterationType)
            throws ExecutionException, InterruptedException {

        int initialPartitionStateVersion = partitionService.getPartitionStateVersion();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);

        // first we optimize the query
        predicate = queryOptimizer.optimize(predicate, mapContainer.getIndexes());

        // then we try to run using an index, but if that doesn't work, we'll try a full table scan
        // This would be the point where a query-plan should be added. It should determine if a full table scan
        // or an index should be used.
        QueryResult result = tryQueryUsingIndexes(predicate, initialPartitions, mapContainer, iterationType);
        if (result == null) {
            result = queryUsingFullTableScan(mapName, predicate, initialPartitions, iterationType);
        }

        if (hasPartitionVersion(initialPartitionStateVersion, predicate)) {
            result.setPartitionIds(initialPartitions);
        }

        updateStatistics(mapContainer);

        return result;
    }

    protected QueryResult tryQueryUsingIndexes(Predicate predicate, Collection<Integer> partitions, MapContainer mapContainer,
                                               IterationType iterationType) {

        if (partitionService.hasOnGoingMigrationLocal()) {
            return null;
        }

        Set<QueryableEntry> entries = mapContainer.getIndexes().query(predicate);
        if (entries == null) {
            return null;
        }

        QueryResult result = newQueryResult(partitions.size(), iterationType);
        result.addAll(entries);
        return result;
    }

    protected void updateStatistics(MapContainer mapContainer) {
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localStats = localMapStatsProvider.getLocalMapStatsImpl(mapContainer.getName());
            localStats.incrementOtherOperations();
        }
    }

    protected QueryResult queryUsingFullTableScan(String name, Predicate predicate, Collection<Integer> partitions,
                                                  IterationType iterationType)
            throws InterruptedException, ExecutionException {

        if (predicate instanceof PagingPredicate) {
            return queryParallelForPaging(name, (PagingPredicate) predicate, partitions, iterationType);
        } else if (parallelEvaluation) {
            return queryParallel(name, predicate, partitions, iterationType);
        } else {
            return querySequential(name, predicate, partitions, iterationType);
        }
    }

    protected QueryResult querySequential(String name, Predicate predicate, Collection<Integer> partitions,
                                          IterationType iterationType) {

        QueryResult result = newQueryResult(partitions.size(), iterationType);
        RetryableHazelcastException storedException = null;
        for (Integer partitionId : partitions) {
            try {
                Collection<QueryableEntry> entries = queryTheLocalPartition(name, predicate, partitionId);
                result.addAll(entries);
            } catch (RetryableHazelcastException e) {
                // RetryableHazelcastException are stored and re-thrown later. this is to ensure all partitions
                // are touched as when the parallel execution was used.
                // see discussion at https://github.com/hazelcast/hazelcast/pull/5049#discussion_r28773099 for details.
                if (storedException == null) {
                    storedException = e;
                }
            }
        }
        if (storedException != null) {
            throw storedException;
        }
        return result;
    }

    protected QueryResult queryParallel(String name, Predicate predicate, Collection<Integer> partitions,
                                        IterationType iterationType) throws InterruptedException, ExecutionException {
        QueryResult result = newQueryResult(partitions.size(), iterationType);

        List<Future<Collection<QueryableEntry>>> futures
                = new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());

        for (Integer partitionId : partitions) {
            QueryPartitionCallable task = new QueryPartitionCallable(name, predicate, partitionId);
            Future<Collection<QueryableEntry>> future = executor.submit(task);
            futures.add(future);
        }

        Collection<Collection<QueryableEntry>> returnedResults = getResult(futures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            if (returnedResult == null) {
                continue;
            }
            result.addAll(returnedResult);
        }

        return result;
    }

    protected QueryResult queryParallelForPaging(String name, PagingPredicate predicate, Collection<Integer> partitions,
                                                 IterationType iterationType) throws InterruptedException, ExecutionException {
        QueryResult result = newQueryResult(partitions.size(), iterationType);

        List<Future<Collection<QueryableEntry>>> futures =
                new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());
        for (Integer partitionId : partitions) {
            QueryPartitionCallable task = new QueryPartitionCallable(name, predicate, partitionId);
            Future<Collection<QueryableEntry>> future = executor.submit(task);
            futures.add(future);
        }

        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        Collection<Collection<QueryableEntry>> returnedResults = getResult(futures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            toMerge.addAll(returnedResult);
        }

        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(predicate);
        List<QueryableEntry> sortedSubList = getSortedSubList(toMerge, predicate, nearestAnchorEntry);
        result.addAll(sortedSubList);
        return result;
    }

    protected static Collection<Collection<QueryableEntry>> getResult(List<Future<Collection<QueryableEntry>>> lsFutures) {
        return returnWithDeadline(lsFutures, QUERY_EXECUTION_TIMEOUT_MINUTES, MINUTES, RETHROW_EVERYTHING);
    }

    protected boolean hasPartitionVersion(int expectedVersion, Predicate predicate) {
        if (expectedVersion != partitionService.getPartitionStateVersion()) {
            logger.info("Partition assignments changed while executing query: " + predicate);
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    protected Collection<QueryableEntry> queryTheLocalPartition(String mapName, Predicate predicate, int partitionId) {
        PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryableEntry> resultList = new LinkedList<QueryableEntry>();

        PartitionContainer partitionContainer = mapServiceContext.getPartitionContainer(partitionId);
        MapContainer mapContainer = mapServiceContext.getMapContainer(mapName);
        Iterator<Record> iterator = partitionContainer.getRecordStore(mapName).loadAwareIterator(getNow(), false);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(pagingPredicate);
        boolean useCachedVersion = shouldUseCachedValue(mapContainer);
        Extractors extractors = mapServiceContext.getExtractors(mapName);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Object value = useCachedVersion ? Records.getValueOrCachedValue(record, serializationService) : record.getValue();
            if (value == null) {
                continue;
            }
            Data key = record.getKey();
            //we want to always use CachedQueryEntry as these are short-living objects anyway
            QueryableEntry queryEntry = new CachedQueryEntry(serializationService, key, value, extractors);

            if (predicate.apply(queryEntry) && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {
                resultList.add(queryEntry);
            }
        }
        return getSortedSubList(resultList, pagingPredicate, nearestAnchorEntry);
    }

    private boolean shouldUseCachedValue(MapContainer mapContainer) {
        CacheDeserializedValues cacheDeserializedValues = mapContainer.getMapConfig().getCacheDeserializedValues();
        switch (cacheDeserializedValues) {
            case NEVER:
                return false;
            case ALWAYS:
                return true;
            default:
                //if index exists then cached value is already set -> let's use it
                return mapContainer.getIndexes().hasIndex();
        }
    }

    @Override
    public QueryResult queryLocalPartition(String mapName, Predicate predicate, int partitionId, IterationType iterationType) {
        Collection<QueryableEntry> queryableEntries = queryTheLocalPartition(mapName, predicate, partitionId);
        QueryResult result = newQueryResult(1, iterationType);
        result.addAll(queryableEntries);
        result.setPartitionIds(singletonList(partitionId));
        return result;
    }

    @Override
    public QueryResult invokeQueryLocalPartitions(String mapName, Predicate predicate, IterationType iterationType) {
        checkNotPagingPredicate(predicate);

        List<Integer> partitionIds = getLocalPartitionIds();
        QueryResult result = newQueryResult(partitionIds.size(), iterationType);

        try {
            Future<QueryResult> future = queryOnLocalMember(mapName, predicate, iterationType);
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
            List<Future<QueryResult>> futures = queryPartitions(mapName, predicate, partitionIds, iterationType);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return result;
    }

    @Override
    public Set queryLocalPartitionsWithPagingPredicate(String mapName, PagingPredicate predicate, IterationType iterationType) {
        predicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        List<Integer> partitionIds = getLocalPartitionIds();

        // in case of value, we also need to get the keys for sorting.
        IterationType retrievalIterationType = iterationType == IterationType.VALUE ? IterationType.ENTRY : iterationType;

        try {
            Future<QueryResult> future = queryOnLocalMember(mapName, predicate, retrievalIterationType);
            List<Future<QueryResult>> futures = singletonList(future);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, predicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryPartitions(mapName, predicate, partitionIds, retrievalIterationType);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return getSortedQueryResultSet(resultList, predicate, iterationType);
    }

    @Override
    public Set queryAllPartitionsWithPagingPredicate(String mapName, PagingPredicate predicate, IterationType iterationType) {
        predicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        Set<Integer> partitionIds = getAllPartitionIds();

        // in case of value, we also need to get the keys for sorting.
        IterationType retrievalIterationType = iterationType == IterationType.VALUE ? IterationType.ENTRY : iterationType;

        try {
            List<Future<QueryResult>> futures = queryOnMembers(mapName, predicate, retrievalIterationType);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, predicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryPartitions(mapName, predicate, partitionIds, retrievalIterationType);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return getSortedQueryResultSet(resultList, predicate, iterationType);
    }

    @Override
    public QueryResult invokeQueryAllPartitions(String mapName, Predicate predicate, IterationType iterationType) {
        checkNotPagingPredicate(predicate);
        if (predicate == TruePredicate.INSTANCE) {
            queryResultSizeLimiter.checkMaxResultLimitOnLocalPartitions(mapName);
        }

        Set<Integer> partitionIds = getAllPartitionIds();
        QueryResult result = newQueryResult(partitionIds.size(), iterationType);

        try {
            List<Future<QueryResult>> futures = queryOnMembers(mapName, predicate, iterationType);
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
            List<Future<QueryResult>> futures = queryPartitions(mapName, predicate, partitionIds, iterationType);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw rethrow(t);
        }

        return result;
    }

    /**
     * Creates a {@link QueryResult} with configured result limit (according to the number of partitions) if feature is enabled.
     *
     * @param numberOfPartitions number of partitions to calculate result limit
     * @return {@link QueryResult}
     */
    protected QueryResult newQueryResult(int numberOfPartitions, IterationType iterationType) {
        return new QueryResult(iterationType, queryResultSizeLimiter.getNodeResultLimit(numberOfPartitions));
    }

    protected void checkNotPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Predicate should not be a paging predicate");
        }
    }

    protected Future<QueryResult> queryOnLocalMember(String mapName, Predicate predicate, IterationType iterationType) {
        Operation operation = new QueryOperation(mapName, predicate, iterationType);
        return operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    protected List<Future<QueryResult>> queryOnMembers(String mapName, Predicate predicate, IterationType iterationType) {
        Collection<Member> members = clusterService.getMembers();
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(members.size());
        for (Member member : members) {
            Operation operation = new QueryOperation(mapName, predicate, iterationType);
            Future<QueryResult> future = operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    protected List<Future<QueryResult>> queryPartitions(String mapName, Predicate predicate,
                                                        Collection<Integer> partitionIds, IterationType iterationType) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            Operation op = new QueryPartitionOperation(mapName, predicate, iterationType);
            op.setPartitionId(partitionId);
            try {
                Future<QueryResult> future = operationService
                        .invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
        return futures;
    }

    /**
     * Adds results of paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    protected void addResultsOfPagingPredicate(List<Future<QueryResult>> futures, Collection result,
                                               Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
            if (queryResult == null) {
                continue;
            }

            Collection<Integer> tmpPartitionIds = queryResult.getPartitionIds();
            if (tmpPartitionIds != null) {
                partitionIds.removeAll(tmpPartitionIds);
                for (QueryResultRow row : queryResult.getRows()) {
                    Object key = toObject(row.getKey());
                    Object value = toObject(row.getValue());
                    result.add(new AbstractMap.SimpleImmutableEntry<Object, Object>(key, value));
                }
            }
        }
    }

    /**
     * Adds results of non-paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    protected void addResultsOfPredicate(List<Future<QueryResult>> futures, QueryResult result,
                                         Collection<Integer> partitionIds) throws ExecutionException, InterruptedException {

        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
            if (queryResult == null) {
                continue;
            }
            Collection<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                partitionIds.removeAll(queriedPartitionIds);
                result.addAllRows(queryResult.getRows());
            }
        }
    }

    protected Object toObject(Object obj) {
        return serializationService.toObject(obj);
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

    protected long getNow() {
        return Clock.currentTimeMillis();
    }

    protected final class QueryPartitionCallable implements Callable<Collection<QueryableEntry>> {

        protected final int partition;
        protected final String name;
        protected final Predicate predicate;

        protected QueryPartitionCallable(String name, Predicate predicate, int partitionId) {
            this.name = name;
            this.predicate = predicate;
            this.partition = partitionId;
        }

        @Override
        public Collection<QueryableEntry> call() throws Exception {
            MapQueryEngineImpl queryEngine = (MapQueryEngineImpl) mapServiceContext.getMapQueryEngine(name);
            return queryEngine.queryTheLocalPartition(name, predicate, partition);
        }
    }
}
