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

import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.PagingPredicateAccessor;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryEntry;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.OperationService;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;
import com.hazelcast.util.SortedQueryResultSet;
import com.hazelcast.util.SortingUtil;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Support methods which are used in map specific query operations.
 */
class BasicMapContextQuerySupport implements MapContextQuerySupport {

    private final MapServiceContext mapServiceContext;
    private final NodeEngine nodeEngine;

    public BasicMapContextQuerySupport(MapServiceContext mapServiceContext) {
        this.mapServiceContext = mapServiceContext;
        this.nodeEngine = mapServiceContext.getNodeEngine();
    }

    @Override
    public Collection<QueryableEntry> queryOnPartition(String mapName, Predicate predicate, int partitionId) {
        SerializationService serializationService = nodeEngine.getSerializationService();
        PagingPredicate pagingPredicate = predicate instanceof PagingPredicate ? (PagingPredicate) predicate : null;
        List<QueryEntry> queryEntries = new LinkedList<QueryEntry>();

        PartitionContainer container = mapServiceContext.getPartitionContainer(partitionId);
        Iterator<Record> iterator = container.getRecordStore(mapName).loadAwareIterator(getNow(), false);
        while (iterator.hasNext()) {
            Record record = iterator.next();
            Data key = record.getKey();
            Object value = getValueOrCachedValue(record);
            if (value == null) {
                continue;
            }
            QueryEntry queryEntry = new QueryEntry(serializationService, key, key, value);
            if (predicate.apply(queryEntry)) {
                if (pagingPredicate != null) {
                    Map.Entry anchor = pagingPredicate.getAnchor();
                    if (anchor != null
                            && SortingUtil.compare(pagingPredicate.getComparator(),
                            pagingPredicate.getIterationType(), anchor, queryEntry) >= 0) {
                        continue;
                    }
                }
                queryEntries.add(queryEntry);
            }
        }

        return getPage(queryEntries, pagingPredicate);
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

        NodeEngine nodeEngine = this.nodeEngine;
        SerializationService serializationService = nodeEngine.getSerializationService();
        Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        List<Integer> partitionIds = getLocalPartitionIds(nodeEngine);

        try {
            Future future = queryOnLocalMember(mapName, predicate, nodeEngine);
            List<Future> futures = Collections.singletonList(future);
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Could not get results", t);
        }

        try {
            List<Future> futures = queryOnPartitions(mapName, predicate, nodeEngine, partitionIds);
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
        setPreviousPagesAnchorsOnLocalMember(mapName, pagingPredicate, iterationType);
        Set result = new SortedQueryResultSet(pagingPredicate.getComparator(), iterationType, pagingPredicate.getPageSize());

        NodeEngine nodeEngine = this.nodeEngine;
        List<Integer> partitionIds = getLocalPartitionIds(nodeEngine);

        try {
            Future future = queryOnLocalMember(mapName, pagingPredicate, nodeEngine);
            List<Future> futures = Collections.singletonList(future);
            addResultsOfPagingPredicate(futures, result, partitionIds);

            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate,
                        ((SortedQueryResultSet) result).last());
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Could not get results", t);
        }

        try {
            List<Future> futures = queryOnPartitions(mapName, pagingPredicate, nodeEngine, partitionIds);
            addResultsOfPagingPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        return result;
    }

    /**
     * Used for paging predicate queries on all members.
     *
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    @Override
    public Set queryWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType) {
        pagingPredicate.setIterationType(iterationType);
        setPreviousPagesAnchors(mapName, pagingPredicate, iterationType);

        NodeEngine nodeEngine = this.nodeEngine;
        Set result = new SortedQueryResultSet(pagingPredicate.getComparator(), iterationType, pagingPredicate.getPageSize());
        Set<Integer> partitionIds = getAllPartitionIds(nodeEngine);

        try {
            List<Future> futures = queryOnMembers(mapName, pagingPredicate, nodeEngine);
            addResultsOfPagingPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Could not get results", t);
        }

        try {
            List<Future> futures = queryOnPartitions(mapName, pagingPredicate, nodeEngine, partitionIds);
            addResultsOfPagingPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        PagingPredicateAccessor.setPagingPredicateAnchor(pagingPredicate, ((SortedQueryResultSet) result).last());

        return result;
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

        NodeEngine nodeEngine = this.nodeEngine;
        SerializationService serializationService = nodeEngine.getSerializationService();
        Set result = new QueryResultSet(serializationService, iterationType, dataResult);
        Set<Integer> partitionIds = getAllPartitionIds(nodeEngine);

        try {
            List<Future> futures = queryOnMembers(mapName, predicate, nodeEngine);
            addResultsOfPredicate(futures, result, partitionIds);
            if (partitionIds.isEmpty()) {
                return result;
            }
        } catch (Throwable t) {
            nodeEngine.getLogger(getClass()).warning("Could not get results", t);
        }

        try {
            List<Future> futures = queryOnPartitions(mapName, predicate, nodeEngine, partitionIds);
            addResultsOfPredicate(futures, result, partitionIds);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }

        return result;
    }

    private Future queryOnLocalMember(String mapName, Predicate predicate, NodeEngine nodeEngine) {
        return nodeEngine
                .getOperationService()
                .invokeOnTarget(MapService.SERVICE_NAME, new QueryOperation(mapName, predicate), nodeEngine.getThisAddress());
    }

    private List<Future> queryOnMembers(String mapName, Predicate predicate, NodeEngine nodeEngine) {
        OperationService operationService = nodeEngine.getOperationService();
        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        List<Future> futures = new ArrayList<Future>(members.size());
        for (MemberImpl member : members) {
            Future future = operationService
                    .invokeOnTarget(MapService.SERVICE_NAME, new QueryOperation(mapName, predicate), member.getAddress());
            futures.add(future);
        }
        return futures;
    }

    private List<Future> queryOnPartitions(String mapName, Predicate predicate, NodeEngine nodeEngine,
                                           Collection<Integer> partitionIds) {
        if (partitionIds == null || partitionIds.isEmpty()) {
            return Collections.emptyList();
        }

        OperationService operationService = nodeEngine.getOperationService();
        List<Future> futures = new ArrayList<Future>(partitionIds.size());
        for (Integer partitionId : partitionIds) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(mapName, predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(MapService.SERVICE_NAME, queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
        return futures;
    }

    /**
     * For paging predicates.
     * Adds results to result set and removes queried partition ids.
     */
    private void addResultsOfPagingPredicate(List<Future> futures, Set result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future future : futures) {
            QueryResult queryResult = getQueryResult(future);
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
     * For predicates except paging predicates.
     * Adds results to result set and removes queried partition ids.
     */
    private void addResultsOfPredicate(List<Future> futures, Set result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future future : futures) {
            QueryResult queryResult = getQueryResult(future);
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

    private QueryResult getQueryResult(Future future) throws ExecutionException, InterruptedException {
        return (QueryResult) future.get();
    }

    private Object toObject(Object obj) {
        return nodeEngine.getSerializationService().toObject(obj);
    }

    private List<Integer> getLocalPartitionIds(NodeEngine nodeEngine) {
        return nodeEngine.getPartitionService().getMemberPartitions(nodeEngine.getThisAddress());
    }

    private Set<Integer> getAllPartitionIds(NodeEngine nodeEngine) {
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

    private List getPage(List<QueryEntry> queryEntries, PagingPredicate pagingPredicate) {
        if (pagingPredicate == null) {
            return queryEntries;
        }

        Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        Collections.sort(queryEntries, wrapperComparator);
        if (queryEntries.size() > pagingPredicate.getPageSize()) {
            queryEntries = queryEntries.subList(0, pagingPredicate.getPageSize());
        }
        return queryEntries;
    }

    private void checkIfNotPagingPredicate(Predicate predicate) {
        if (predicate instanceof PagingPredicate) {
            throw new IllegalArgumentException("Predicate should not be a type of paging predicate");
        }
    }

    private void setPreviousPagesAnchorsOnLocalMember(String mapName,
                                                      PagingPredicate pagingPredicate, IterationType iterationType) {
        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            queryLocalMemberWithPagingPredicate(mapName, pagingPredicate, iterationType);
            pagingPredicate.nextPage();
        }
    }

    private void setPreviousPagesAnchors(String mapName, PagingPredicate pagingPredicate, IterationType iterationType) {
        if (pagingPredicate.getPage() > 0 && pagingPredicate.getAnchor() == null) {
            pagingPredicate.previousPage();
            queryWithPagingPredicate(mapName, pagingPredicate, iterationType);
            pagingPredicate.nextPage();
        }
    }

    private long getNow() {
        return Clock.currentTimeMillis();
    }
}
