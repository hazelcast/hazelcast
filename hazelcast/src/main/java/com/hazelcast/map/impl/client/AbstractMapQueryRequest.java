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

package com.hazelcast.map.impl.client;

import static com.hazelcast.map.impl.MapService.SERVICE_NAME;

import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.map.impl.operation.QueryOperation;
import com.hazelcast.map.impl.operation.QueryPartitionOperation;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.QueryResultSet;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

abstract class AbstractMapQueryRequest extends InvocationClientRequest implements Portable, SecureRequest,
        RetryableRequest {

    protected IterationType iterationType;
    private String name;

    public AbstractMapQueryRequest() {
    }

    public AbstractMapQueryRequest(String name, IterationType iterationType) {
        this.name = name;
        this.iterationType = iterationType;
    }

    @Override
    protected final void invoke() {
        QueryResultSet result = new QueryResultSet(null, iterationType, true);
        try {
            Predicate predicate = getPredicate();

            Collection<MemberImpl> members = getClientEngine().getClusterService().getMemberList();
            List<Future> futures = new ArrayList<Future>();
            createInvocations(members, futures, predicate);

            int partitionCount = getClientEngine().getPartitionService().getPartitionCount();
            Set<Integer> finishedPartitions = new HashSet<Integer>(partitionCount);
            collectResults(result, futures, finishedPartitions);

            if (hasMissingPartitions(finishedPartitions, partitionCount)) {
                List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
                List<Future> missingFutures = new ArrayList<Future>(missingList.size());
                createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
                collectResultsFromMissingPartitions(result, missingFutures);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        getEndpoint().sendResponse(result, getCallId());
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> futures, Predicate predicate) {
        for (MemberImpl member : members) {
            Future future = createInvocationBuilder(SERVICE_NAME, new QueryOperation(name, predicate),
                    member.getAddress()).invoke();
            futures.add(future);
        }
    }

    private void collectResults(QueryResultSet result, List<Future> futures, Set<Integer> finishedPartitions)
            throws InterruptedException, java.util.concurrent.ExecutionException {

        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
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
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
            queryPartitionOperation.setPartitionId(partitionId);
            try {
                Future future = createInvocationBuilder(SERVICE_NAME, queryPartitionOperation, partitionId).invoke();
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    private void collectResultsFromMissingPartitions(QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            result.addAll(queryResult.getResult());
        }
    }

    @Override
    public final int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    @Override
    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }

    @Override
    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("t", iterationType.toString());
        writePortableInner(writer);
    }

    @Override
    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        iterationType = IterationType.valueOf(reader.readUTF("t"));
        readPortableInner(reader);
    }

    protected abstract Predicate getPredicate();

    protected abstract void writePortableInner(PortableWriter writer) throws IOException;

    protected abstract void readPortableInner(PortableReader reader) throws IOException;
}
