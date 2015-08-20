/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl;

import com.hazelcast.core.Member;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.util.SortingUtil.compareAnchor;
import static com.hazelcast.util.SortingUtil.getSortedQueryResultSet;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

/**
 * Support methods which are used in map specific query operations.
 */
class BasicMapContextQuerySupport implements MapContextQuerySupport {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private final QueryResultSizeLimiter queryResultSizeLimiter;

    public BasicMapContextQuerySupport(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
        this.logger = nodeEngine.getLogger(getClass());
        this.queryResultSizeLimiter = new QueryResultSizeLimiter(mapServiceContext, logger);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<QueryableEntry> queryOnPartition(String mapName, Predicate predicate, int partitionId) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryableEntry> resultList = new LinkedList<QueryableEntry>();

        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        Iterator<Record> iterator = container.getRecordStore(mapName).loadAwareIterator(getNow(), false);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = PagingPredicateAccessor.getNearestAnchorEntry(pagingPredicate);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            Object value = getValueOrCachedValue(record);
            if (value == null) {
                continue;
            }
            QueryEntry queryEntry = new QueryEntry(serializationService, key, key, value);
            if (predicate.apply(queryEntry) && compareAnchor(pagingPredicate, queryEntry, nearestAnchorEntry)) {
                resultList.add(queryEntry);
            }
        }
        return getSortedSubList(resultList, pagingPredicate, nearestAnchorEntry);
    }

    private Object getValueOrCachedValue(Record record) {
        Object value = record.getCachedValue();
        if (value == Record.NOT_CACHED) {
            value = record.getValue();
        } else if (value == null) {
            value = record.getValue();
            if (value instanceof Data && !((Data) value).isPortable()) {
                value = nodeEngine.getSerializationService().toObject(value);
                record.setCachedValue(value);
            }
        }
        return value;
    }

    /**
     * Used for predicates which queries on node local entries, except paging predicate.
     *
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link com.hazelcast.util.IterationType}
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link com.hazelcast.util.QueryResultSet}
     */
    @Override
    public Set queryLocalMember(String mapName, Predicate predicate, IterationType iterationType, boolean dataResult) {
        checkIfNotPagingPredicate(predicate);

        SerializationService serializationService = nodeEngine.getSerializationService();
        Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        List<Integer> partitionIds = getLocalPartitionIds();

        try {
            Future<QueryResult> future = queryOnLocalMember(mapName, predicate);
            List<Future<QueryResult>> futures = Collections.singletonList(future);
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw ExceptionUtil.rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryOnPartitions(mapName, predicate, partitionIds);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        return result;
    }

    /**
     * Used for paging predicate queries on node local entries.
     *
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link com.hazelcast.util.SortedQueryResultSet}
     */
    @Override
    public Set queryLocalMemberWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType) {
        pagingPredicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        List<Integer> partitionIds = getLocalPartitionIds();

        try {
            Future<QueryResult> future = queryOnLocalMember(mapName, pagingPredicate);
            List<Future<QueryResult>> futures = Collections.singletonList(future);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, pagingPredicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw ExceptionUtil.rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryOnPartitions(mapName, pagingPredicate, partitionIds);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return getSortedQueryResultSet(resultList, pagingPredicate, iterationType);
    }

    /**
     * Used for paging predicate queries on all members.
     *
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link com.hazelcast.util.SortedQueryResultSet}
     */
    @Override
    public Set queryWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType) {
        pagingPredicate.setIterationType(iterationType);
        ArrayList<Map.Entry> resultList = new ArrayList<Map.Entry>();
        Set<Integer> partitionIds = getAllPartitionIds();

        try {
            List<Future<QueryResult>> futures = queryOnMembers(mapName, pagingPredicate);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
            if (partitionIds.isEmpty()) {
                return getSortedQueryResultSet(resultList, pagingPredicate, iterationType);
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw ExceptionUtil.rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryOnPartitions(mapName, pagingPredicate, partitionIds);
            addResultsOfPagingPredicate(futures, resultList, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        return getSortedQueryResultSet(resultList, pagingPredicate, iterationType);
    }

    /**
     * Used for predicates which queries on all members, except paging predicate.
     *
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link IterationType}
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link QueryResultSet}
     */
    @Override
    public Set query(String mapName, Predicate predicate, IterationType iterationType, boolean dataResult) {
        checkIfNotPagingPredicate(predicate);
        if (predicate == TruePredicate.INSTANCE) {
            queryResultSizeLimiter.checkMaxResultLimitOnLocalPartitions(mapName);
        }

        SerializationService serializationService = nodeEngine.getSerializationService();
        Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        Set<Integer> partitionIds = getAllPartitionIds();

        try {
            List<Future<QueryResult>> futures = queryOnMembers(mapName, predicate);
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            if (t.getCause() instanceof QueryResultSizeExceededException) {
                throw ExceptionUtil.rethrow(t);
            }
            logger.warning("Could not get results", t);
        }

        try {
            List<Future<QueryResult>> futures = queryOnPartitions(mapName, predicate, partitionIds);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        return result;
    }

    @Override
    public QueryResult newQueryResult(int numberOfPartitions) {
        return new QueryResult(queryResultSizeLimiter.getNodeResultLimit(numberOfPartitions));
    }

    private void checkIfNotPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Predicate should not be a type of paging predicate");
        }
    }

    private Future<QueryResult> queryOnLocalMember(String mapName, Predicate predicate) {
        QueryOperation operation = new QueryOperation(mapName, predicate);
        return nodeEngine.getOperationService().invokeOnTarget(MapService.SERVICE_NAME, operation, nodeEngine.getThisAddress());
    }

    private List<Future<QueryResult>> queryOnMembers(String mapName, Predicate predicate) {
        OperationService operationService = nodeEngine.getOperationService();
        Collection<Member> members = nodeEngine.getClusterService().getMembers();
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(members.size());
        for (Member member : members) {
            QueryOperation operation = new QueryOperation(mapName, predicate);
            Future<QueryResult> future = operationService.invokeOnTarget(MapService.SERVICE_NAME, operation, member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    private List<Future<QueryResult>> queryOnPartitions(String mapName, Predicate predicate, Collection<Integer> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        OperationService operationService = nodeEngine.getOperationService();
        List<Future<QueryResult>> futures = new ArrayList<Future<QueryResult>>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(mapName, predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future<QueryResult> future = operationService
                        .invokeOnPartition(MapService.SERVICE_NAME, queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return futures;
    }

    /**
     * Adds results of paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    private void addResultsOfPagingPredicate(List<Future<QueryResult>> futures, Collection result,
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
                for (QueryResultEntry queryResultEntry : queryResult.getResult()) {
                    Object key = toObject(queryResultEntry.getKeyData());
                    Object value = toObject(queryResultEntry.getValueData());
                    result.add(new AbstractMap.SimpleImmutableEntry<Object, Object>(key, value));
                }
            }
        }
    }

    /**
     * Adds results of non-paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    private void addResultsOfPredicate(List<Future<QueryResult>> futures, Set result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
            if (queryResult == null) {
                continue;
            }
            Collection<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                partitionIds.removeAll(queriedPartitionIds);
                result.addAll(queryResult.getResult());
            }
        }
    }

    private Object toObject(Object obj) {
        return nodeEngine.getSerializationService().toObject(obj);
    }

    private List<Integer> getLocalPartitionIds() {
        return nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
    }

    private Set<Integer> getAllPartitionIds() {
        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        return createSetWithPopulatedPartitionIds(partitionCount);
    }

    private Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
        Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }
}
