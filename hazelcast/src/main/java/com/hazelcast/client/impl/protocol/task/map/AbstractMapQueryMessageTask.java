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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.task.AbstractCallableMessageTask;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryResultEntry;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.util.ExceptionUtil;

import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

public abstract class AbstractMapQueryMessageTask<P> extends AbstractCallableMessageTask<P> {

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

    @Override
    protected final ClientMessage call() throws Exception {
        Collection<QueryResultEntry> result = new LinkedList<QueryResultEntry>();

        Predicate predicate = getPredicate();

        Collection<MemberImpl> members = nodeEngine.getClusterService().getMemberList();
        List<Future> futures = new ArrayList<Future>();
        createInvocations(members, futures, predicate);

        int partitionCount = nodeEngine.getPartitionService().getPartitionCount();
        Set<Integer> finishedPartitions = new HashSet<Integer>(partitionCount);
        collectResults(result, futures, finishedPartitions);

        if (hasMissingPartitions(finishedPartitions, partitionCount)) {
            List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
            collectResultsFromMissingPartitions(result, missingFutures);
        }
        return reduce(result);
    }

    protected abstract Predicate getPredicate();

    protected abstract ClientMessage reduce(Collection<QueryResultEntry> result);

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures, Predicate predicate) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        for (MemberImpl member : members) {
            Future future = operationService.createInvocationBuilder(SERVICE_NAME,
                    new QueryOperation(getDistributedObjectName(), predicate),
                    member.getAddress()).invoke();
            futures.add(future);
        }
    }

    private void collectResults(Collection<QueryResultEntry> result, List<Future> futures, Set<Integer> finishedPartitions)
            throws InterruptedException, java.util.concurrent.ExecutionException {

        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {

                if (queryResult.isResultLimitExceeded()) {
                    throw new QueryResultSizeExceededException();
                }

                Collection<Integer> partitionIds = queryResult.getPartitionIds();
                if (partitionIds != null) {
                    finishedPartitions.addAll(partitionIds);
                    result.addAll(queryResult.getResult());
                }
            }
        }
    }

    private boolean hasMissingPartitions(Set<Integer> finishedPartitions, int partitionCount) {
        return finishedPartitions.size() != partitionCount;
    }

    private List<Integer> findMissingPartitions(Set<Integer> finishedPartitions, int partitionCount) {
        List<Integer> missingList = new ArrayList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            if (!finishedPartitions.contains(i)) {
                missingList.add(i);
            }
        }
        return missingList;
    }

    private void createInvocationsForMissingPartitions(List<Integer> missingPartitionsList, List<Future> futures,
                                                       Predicate predicate) {
        final InternalOperationService operationService = nodeEngine.getOperationService();
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(getDistributedObjectName(), predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = operationService.invokeOnPartition(SERVICE_NAME,
                        queryPartitionOperation, partitionId);
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    private void collectResultsFromMissingPartitions(Collection<QueryResultEntry> result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();

            if (queryResult.isResultLimitExceeded()) {
                throw new QueryResultSizeExceededException();
            }

            result.addAll(queryResult.getResult());
        }
    }
}
