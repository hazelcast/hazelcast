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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.aggregation.Aggregator;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.core.Member;
import com.hazelcast.instance.Node;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.Result;
import com.hazelcast.nio.Connection;
import com.hazelcast.projection.Projection;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.BitSetUtils;
import com.hazelcast.util.IterationType;

import java.security.Permission;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.BitSetUtils.hasAtLeastOneBitSet;
import static com.hazelcast.util.ExceptionUtil.rethrow;

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
            int partitionCount = clientEngine.getPartitionService().getPartitionCount();

            BitSet finishedPartitions = invokeOnMembers(result, predicate, partitionCount);
            invokeOnMissingPartitions(result, predicate, finishedPartitions, partitionCount);
        } catch (Throwable t) {
            throw rethrow(t);
        }
        return reduce(result);
    }

    private BitSet invokeOnMembers(Collection<AccumulatedResults> result, Predicate predicate, int partitionCount)
            throws InterruptedException, ExecutionException {
        Collection<Member> members = clientEngine.getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        List<Future> futures = createInvocations(members, predicate);
        return collectResults(result, futures, partitionCount);
    }

    private void invokeOnMissingPartitions(Collection<AccumulatedResults> result, Predicate predicate,
                                           BitSet finishedPartitions, int partitionCount)
            throws InterruptedException, ExecutionException {
        if (hasMissingPartitions(finishedPartitions, partitionCount)) {
            List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
            collectResultsFromMissingPartitions(finishedPartitions, result, missingFutures);
        }
    }

    private List<Future> createInvocations(Collection<Member> members, Predicate predicate) {
        List<Future> futures = new ArrayList<Future>(members.size());
        final InternalOperationService operationService = nodeEngine.getOperationService();
        final Query query = buildQuery(predicate);
        for (Member member : members) {
            try {
                Future future = operationService.createInvocationBuilder(SERVICE_NAME,
                        new QueryOperation(query), member.getAddress())
                        .invoke();
                futures.add(future);
            } catch (Throwable t) {
                if (t.getCause() instanceof QueryResultSizeExceededException) {
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
        Query.QueryBuilder builder =
            Query.of().mapName(getDistributedObjectName())
                      .predicate(predicate)
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
    private BitSet collectResults(Collection<AccumulatedResults> result, List<Future> futures, int partitionCount)
            throws InterruptedException, ExecutionException {
        BitSet finishedPartitions = new BitSet(partitionCount);
        for (Future future : futures) {
            try {
                QueryResult queryResult = (QueryResult) future.get();
                if (queryResult != null) {
                    Collection<Integer> partitionIds = queryResult.getPartitionIds();
                    if (partitionIds != null && !hasAtLeastOneBitSet(finishedPartitions, partitionIds)) {
                        // Collect results only if there is no overlap with already collected partitions.
                        // If there is an overlap it means there was a partition migration while QueryOperation(s) were
                        // running. In this case we discard all results from this member and will target the missing
                        // partition separately later.
                        BitSetUtils.setBits(finishedPartitions, partitionIds);
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

    private boolean hasMissingPartitions(BitSet finishedPartitions, int partitionCount) {
        return finishedPartitions.nextClearBit(0) < partitionCount;
    }

    private List<Integer> findMissingPartitions(BitSet finishedPartitions, int partitionCount) {
        List<Integer> missingList = new ArrayList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            if (!finishedPartitions.get(i)) {
                missingList.add(i);
            }
        }
        return missingList;
    }

    private void createInvocationsForMissingPartitions(List<Integer> missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {

        final InternalOperationService operationService = nodeEngine.getOperationService();
        Query query = buildQuery(predicate);
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(query);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(SERVICE_NAME,
                        queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw rethrow(t);
            }
        }
    }

    private void collectResultsFromMissingPartitions(BitSet finishedPartitions, Collection<AccumulatedResults> result,
                                                     List<Future> futures)
            throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult.getPartitionIds() != null && queryResult.getPartitionIds().size() > 0
                    && !hasAtLeastOneBitSet(finishedPartitions, queryResult.getPartitionIds())) {
                extractAndAppendResult(result, queryResult);
                BitSetUtils.setBits(finishedPartitions, queryResult.getPartitionIds());
            }
        }
    }
}
