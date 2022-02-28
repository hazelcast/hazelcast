/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.SetUtil;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.Result;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.PartitionPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.QueryException;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.internal.util.IterationType;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.IntConsumer;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

public abstract class AbstractMapQueryMessageTask<P, QueryResult extends Result, AccumulatedResults, ReducedResult>
        extends AbstractCallableMessageTask<P> {

    protected AbstractMapQueryMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(getDistributedObjectName(), ActionConstants.ACTION_READ);
    }

    protected abstract Predicate getPredicate();

    protected abstract Aggregator<?, ?> getAggregator();

    protected abstract Projection<?, ?> getProjection();

    protected abstract void extractAndAppendResult(Collection<AccumulatedResults> results, QueryResult queryResult);

    protected abstract ReducedResult reduce(Collection<AccumulatedResults> results);

    protected abstract IterationType getIterationType();

    @Override
    protected final Object call() throws Exception {
        Collection<AccumulatedResults> result = new LinkedList<AccumulatedResults>();
        try {
            Predicate predicate = getPredicate();
            if (predicate instanceof PartitionPredicate) {
                int partitionId = clientMessage.getPartitionId();
                QueryResult queryResult = invokeOnPartition((PartitionPredicate) predicate, partitionId);
                extractAndAppendResult(result, queryResult);
                return reduce(result);
            }
            int partitionCount = clientEngine.getPartitionService().getPartitionCount();

            PartitionIdSet finishedPartitions = invokeOnMembers(result, predicate, partitionCount);
            invokeOnMissingPartitions(result, predicate, finishedPartitions);
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return reduce(result);
    }

    private QueryResult invokeOnPartition(PartitionPredicate predicate, int partitionId) {
        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        Query query = buildQuery(predicate);
        MapOperation queryPartitionOperation = createQueryPartitionOperation(query, mapServiceContext);
        queryPartitionOperation.setPartitionId(partitionId);
        try {
            return (QueryResult) operationService.invokeOnPartition(SERVICE_NAME, queryPartitionOperation, partitionId).get();
        } catch (Throwable t) {
            throw rethrow(t);
        }
    }

    private PartitionIdSet invokeOnMembers(Collection<AccumulatedResults> result, Predicate predicate, int partitionCount) {
        Collection<Member> members = clientEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        List<Future> futures = createInvocations(members, predicate);
        return collectResults(result, futures, partitionCount);
    }

    private void invokeOnMissingPartitions(Collection<AccumulatedResults> result, Predicate predicate,
                                           PartitionIdSet finishedPartitions)
            throws InterruptedException, ExecutionException {
        if (finishedPartitions.isMissingPartitions()) {
            PartitionIdSet missingPartitions = new PartitionIdSet(finishedPartitions);
            missingPartitions.complement();
            List<Future> missingFutures = new ArrayList<>(missingPartitions.size());
            createInvocationsForMissingPartitions(missingPartitions, missingFutures, predicate);
            collectResultsFromMissingPartitions(finishedPartitions, result, missingFutures);
        }
        assertAllPartitionsQueried(finishedPartitions);
    }

    private List<Future> createInvocations(Collection<Member> members, Predicate predicate) {
        List<Future> futures = new ArrayList<>(members.size());
        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        final Query query = buildQuery(predicate);

        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        for (Member member : members) {
            try {
                Future future = operationService.createInvocationBuilder(SERVICE_NAME,
                        createQueryOperation(query, mapServiceContext),
                        member.getAddress())
                        .invoke();
                futures.add(future);
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
                        logger.fine("Query invocation failed on member " + member, t);
                    }
                }
            }
        }
        return futures;
    }

    private Query buildQuery(Predicate predicate) {
        Query.QueryBuilder builder = Query.of()
                .mapName(getDistributedObjectName())
                .predicate(predicate instanceof PartitionPredicate ? ((PartitionPredicate) predicate).getTarget() : predicate)
                .partitionIdSet(SetUtil.allPartitionIds(nodeEngine.getPartitionService().getPartitionCount()))
                .iterationType(getIterationType());
        if (getAggregator() != null) {
            builder = builder.aggregator(getAggregator());
        }

        if (getProjection() != null) {
            builder = builder.projection(getProjection());
        }

        return builder.build();
    }

    @SuppressWarnings("unchecked")
    private PartitionIdSet collectResults(Collection<AccumulatedResults> result, List<Future> futures, int partitionCount) {
        PartitionIdSet finishedPartitions = new PartitionIdSet(partitionCount);
        for (Future future : futures) {
            try {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    PartitionIdSet partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null && !partitionIds.intersects(finishedPartitions)) {
                        // Collect results only if there is no overlap with already collected partitions.
                        // If there is an overlap it means there was a partition migration while QueryOperation(s) were
                        // running. In this case we discard all results from this member and will target the missing
                        // partition separately later.
                        finishedPartitions.union(partitionIds);
                        extractAndAppendResult(result, queryResult);
                    }
                }
            } catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException) {
                    throw rethrow(t);
                } else {
                    // log failure to invoke query on member at fine level
                    // the missing partition IDs will be queried anyway, so it's not a terminal failure
                    if (logger.isFineEnabled()) {
                        logger.fine("Query on member failed with exception", t);
                    }
                }
            }
        }
        return finishedPartitions;
    }

    private void createInvocationsForMissingPartitions(PartitionIdSet missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {

        final OperationServiceImpl operationService = nodeEngine.getOperationService();
        MapService mapService = nodeEngine.getService(getServiceName());
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();

        Query query = buildQuery(predicate);
        PrimitiveIterator.OfInt missingPartitionIterator = missingPartitionsList.intIterator();
        missingPartitionIterator.forEachRemaining((IntConsumer) partitionId -> {
            MapOperation queryPartitionOperation = createQueryPartitionOperation(query, mapServiceContext);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(SERVICE_NAME,
                        queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        });
    }

    private void collectResultsFromMissingPartitions(PartitionIdSet finishedPartitions, Collection<AccumulatedResults> result,
                                                     List<Future> futures)
            throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult.getPartitionIds() != null && queryResult.getPartitionIds().size() > 0
                    && !finishedPartitions.intersects(queryResult.getPartitionIds())) {
                extractAndAppendResult(result, queryResult);
                finishedPartitions.addAll(queryResult.getPartitionIds());
            }
        }
    }

    private Operation createQueryOperation(Query query, MapServiceContext mapServiceContext) {
        return mapServiceContext.getMapOperationProvider(query.getMapName()).createQueryOperation(query);
    }

    private MapOperation createQueryPartitionOperation(Query query, MapServiceContext mapServiceContext) {
        return mapServiceContext.getMapOperationProvider(
                query.getMapName()).createQueryPartitionOperation(query);
    }

    private void assertAllPartitionsQueried(PartitionIdSet finishedPartitions) {
        if (finishedPartitions.isMissingPartitions()) {
            int missedPartitionsCount = 0;
            int partitionCount = finishedPartitions.getPartitionCount();
            for (int i = 0; i < partitionCount; i++) {
                if (!finishedPartitions.contains(i)) {
                    missedPartitionsCount++;
                }
            }
            throw new QueryException("Query aborted. Could not execute query for all partitions. Missed "
                    + missedPartitionsCount + " partitions");
        }
    }

}
