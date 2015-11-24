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

import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.core.Member;
import com.hazelcast.map.impl.MapPortableHook;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.query.QueryOperation;
import com.hazelcast.map.impl.query.QueryPartitionOperation;
import com.hazelcast.map.impl.query.QueryResult;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.query.Predicate;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.spi.Operation;
import com.hazelcast.util.BitSetUtils;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.IterationType;

import java.io.IOException;
import java.security.Permission;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.util.BitSetUtils.hasAtLeastOneBitSet;

abstract class AbstractMapQueryRequest extends InvocationClientRequest implements Portable, SecureRequest,
        RetryableRequest {

    protected IterationType iterationType;

    private String name;

    AbstractMapQueryRequest() {
    }

    AbstractMapQueryRequest(String name, IterationType iterationType) {
        this.name = name;
        this.iterationType = iterationType;
    }

    @Override
    protected final void invoke() {
        QueryResult result = new QueryResult(iterationType, Long.MAX_VALUE);
        try {
            Predicate predicate = getPredicate();
            int partitionCount = getClientEngine().getPartitionService().getPartitionCount();

            BitSet finishedPartitions = invokeOnMembers(result, predicate, partitionCount);
            invokeOnMissingPartitions(result, predicate, finishedPartitions, partitionCount);
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        getEndpoint().sendResponse(result, getCallId());
    }

    private BitSet invokeOnMembers(QueryResult result, Predicate predicate, int partitionCount)
            throws InterruptedException, ExecutionException {
        Collection<Member> members = getClientEngine().getClusterService().getMembers(DATA_MEMBER_SELECTOR);
        List<Future> futures = createInvocations(members, predicate);
        return collectResults(result, futures, partitionCount);
    }

    private void invokeOnMissingPartitions(QueryResult result, Predicate predicate,
                                           BitSet finishedPartitions, int partitionCount)
            throws InterruptedException, ExecutionException {
        if (hasMissingPartitions(finishedPartitions, partitionCount)) {
            List<Integer> missingList = findMissingPartitions(finishedPartitions, partitionCount);
            List<Future> missingFutures = new ArrayList<Future>(missingList.size());
            createInvocationsForMissingPartitions(missingList, missingFutures, predicate);
            collectResultsFromMissingPartitions(result, missingFutures);
        }
    }

    private List<Future> createInvocations(Collection<Member> members, Predicate predicate) {
        String mapName = this.name;
        List<Future> futures = new ArrayList<Future>(members.size());
        for (Member member : members) {
            Operation operation = new QueryOperation(mapName, predicate, iterationType);
            Future future = createInvocationBuilder(SERVICE_NAME, operation, member.getAddress()).invoke();
            futures.add(future);
        }
        return futures;
    }

    private MapOperationProvider getMapOperationProvider(String mapName) {
        MapService mapService = getService();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapName);
    }

    @SuppressWarnings("unchecked")
    private BitSet collectResults(QueryResult result, List<Future> futures, int partitionCount)
            throws InterruptedException, ExecutionException {
        BitSet finishedPartitions = new BitSet(partitionCount);
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                Collection<Integer> partitionIds = queryResult.getPartitionIds();
                if (partitionIds != null && !hasAtLeastOneBitSet(finishedPartitions, partitionIds)) {
                    //Collect results only if there is no overlap with already collected partitions.
                    //If there is an overlap it means there was a partition migration while QueryOperation(s) were
                    //running. In this case we discard all results from this member and will target the missing
                    //partition separately later.
                    BitSetUtils.setBits(finishedPartitions, partitionIds);
                    result.addAllRows(queryResult.getRows());
                }
            }
        }
        return finishedPartitions;
    }

    private boolean hasMissingPartitions(BitSet finishedPartitions, int partitionCount) {
        return finishedPartitions.nextClearBit(0) == partitionCount;
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
        for (Integer partitionId : missingPartitionsList) {
            QueryPartitionOperation op = new QueryPartitionOperation(name, predicate, iterationType);
            op.setPartitionId(partitionId);
            try {
                Future future = createInvocationBuilder(SERVICE_NAME, op, partitionId).invoke();
                futures.add(future);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void collectResultsFromMissingPartitions(QueryResult result, List<Future> futures)
            throws InterruptedException, ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            result.addAllRows(queryResult.getRows());
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
