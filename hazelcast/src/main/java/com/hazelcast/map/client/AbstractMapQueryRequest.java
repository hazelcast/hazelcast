/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.client;

import static com.hazelcast.map.MapService.SERVICE_NAME;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.client.InvocationClientRequest;
import com.hazelcast.client.impl.client.RetryableRequest;
import com.hazelcast.client.impl.client.SecureRequest;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.map.MapPortableHook;
import com.hazelcast.map.MapService;
import com.hazelcast.map.QueryResult;
import com.hazelcast.map.operation.QueryOperation;
import com.hazelcast.map.operation.QueryPartitionOperation;
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

abstract class AbstractMapQueryRequest extends InvocationClientRequest implements Portable,
        RetryableRequest, SecureRequest {

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
        Collection<MemberImpl> members = getClientEngine().getClusterService().getMemberList();
        int partitionCount = getClientEngine().getPartitionService().getPartitionCount();
        Set<Integer> plist = new HashSet<Integer>(partitionCount);
        final ClientEndpoint endpoint = getEndpoint();
        QueryResultSet result = new QueryResultSet(null, iterationType, true);
        try {
            List<Future> futures = new ArrayList<Future>();
            final Predicate predicate = getPredicate();
            createInvocations(members, futures, predicate);
            collectResults(plist, result, futures);
            if (hasMissingPartitions(partitionCount, plist)) {
                List<Integer> missingList = findMissingPartitions(partitionCount, plist);
                List<Future> missingFutures = new ArrayList<Future>(missingList.size());
                createInvocationsForMissingPartitions(predicate, missingList, missingFutures);
                collectResultsFromMissingPartitions(result, missingFutures);
            }
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
        endpoint.sendResponse(result, getCallId());
    }

    private boolean hasMissingPartitions(int partitionCount, Set<Integer> plist) {
        return plist.size() != partitionCount;
    }

    private void collectResultsFromMissingPartitions(QueryResultSet result, List<Future> futures)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : futures) {
            QueryResult queryResult = (QueryResult) future.get();
            result.addAll(queryResult.getResult());
        }
    }

    private void createInvocationsForMissingPartitions(Predicate predicate, List<Integer> missingList,
                                                       List<Future> futures) {
        for (Integer pid : missingList) {
            QueryPartitionOperation queryPartitionOperation = new QueryPartitionOperation(name, predicate);
            queryPartitionOperation.setPartitionId(pid);
            try {
                Future f = createInvocationBuilder(SERVICE_NAME, queryPartitionOperation, pid).invoke();
                futures.add(f);
            } catch (Throwable t) {
                throw ExceptionUtil.rethrow(t);
            }
        }
    }

    private List<Integer> findMissingPartitions(int partitionCount, Set<Integer> plist) {
        List<Integer> missingList = new ArrayList<Integer>();
        for (int i = 0; i < partitionCount; i++) {
            if (!plist.contains(i)) {
                missingList.add(i);
            }
        }
        return missingList;
    }

    private void collectResults(Set<Integer> plist, QueryResultSet result, List<Future> flist)
            throws InterruptedException, java.util.concurrent.ExecutionException {
        for (Future future : flist) {
            QueryResult queryResult = (QueryResult) future.get();
            if (queryResult != null) {
                final Collection<Integer> partitionIds = queryResult.getPartitionIds();
                if (partitionIds != null) {
                    plist.addAll(partitionIds);
                    result.addAll(queryResult.getResult());
                }
            }
        }
    }

    private void createInvocations(Collection<MemberImpl> members, List<Future> flist, Predicate predicate) {
        for (MemberImpl member : members) {
            Future future = createInvocationBuilder(SERVICE_NAME, new QueryOperation(name, predicate),
                    member.getAddress()).invoke();
            flist.add(future);
        }
    }

    protected abstract Predicate getPredicate();

    public final String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    public final int getFactoryId() {
        return MapPortableHook.F_ID;
    }

    public void write(PortableWriter writer) throws IOException {
        writer.writeUTF("n", name);
        writer.writeUTF("t", iterationType.toString());
        writePortableInner(writer);
    }

    protected abstract void writePortableInner(PortableWriter writer) throws IOException;

    public void read(PortableReader reader) throws IOException {
        name = reader.readUTF("n");
        iterationType = IterationType.valueOf(reader.readUTF("t"));
        readPortableInner(reader);
    }

    protected abstract void readPortableInner(PortableReader reader) throws IOException;

    public Permission getRequiredPermission() {
        return new MapPermission(name, ActionConstants.ACTION_READ);
    }

    @Override
    public String getDistributedObjectName() {
        return name;
    }
}
