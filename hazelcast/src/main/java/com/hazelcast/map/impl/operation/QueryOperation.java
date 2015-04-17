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

package com.hazelcast.map.impl.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.impl.MapContextQuerySupport;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.IndexService;
import com.hazelcast.query.impl.QueryResultEntryImpl;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.util.FutureUtil;
import com.hazelcast.util.SortingUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.returnWithDeadline;

public class QueryOperation extends AbstractMapOperation {

    private static final long QUERY_EXECUTION_TIMEOUT_MINUTES = 5;

    private Predicate predicate;
    private PagingPredicate pagingPredicate;

    private QueryResult result;

    private transient boolean nodeResultLimitEnabled;
    private transient long nodeResultLimit;
    private transient long resultSize;

    @SuppressWarnings("unused")
    public QueryOperation() {
    }

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
        if (predicate instanceof PagingPredicate) {
            this.pagingPredicate = (PagingPredicate) predicate;
        }
        this.resultSize = 0;
    }

    @Override
    public void run() throws Exception {
        MapService service = getService();
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapContextQuerySupport mapQuerySupport = mapServiceContext.getMapContextQuerySupport();

        int partitionStateVersion = partitionService.getPartitionStateVersion();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();
        nodeResultLimitEnabled = mapQuerySupport.isQueryResultLimitEnabled();
        nodeResultLimit = nodeResultLimitEnabled ? mapQuerySupport.getNodeResultLimit(initialPartitions.size()) : 0;

        IndexService indexService = mapServiceContext.getMapContainer(name).getIndexService();
        Set<QueryableEntry> entries = null;
        if (!partitionService.hasOnGoingMigrationLocal()) {
            entries = indexService.query(predicate);
        }

        result = new QueryResult();
        if (entries != null) {
            addQueryEntriesToResult(entries);
        } else {
            // run in parallel
            if (pagingPredicate != null) {
                runParallelForPaging(initialPartitions);
            } else {
                runParallel(initialPartitions);
            }
        }
        Collection<Integer> finalPartitions = mapServiceContext.getOwnedPartitions();
        if (initialPartitions.equals(finalPartitions)) {
            result.setPartitionIds(finalPartitions);
        }
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localMapStatsImpl = mapServiceContext
                    .getLocalMapStatsProvider().getLocalMapStatsImpl(name);
            localMapStatsImpl.incrementOtherOperations();
        }

        checkPartitionStateChanges(partitionService, partitionStateVersion);
    }

    private void runParallel(Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(
                initialPartitions.size());

        for (Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(f);
        }

        Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            if (returnedResult == null) {
                continue;
            }
            if (!addQueryEntriesToResult(returnedResult)) {
                break;
            }
        }
    }

    private void runParallelForPaging(Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(
                initialPartitions.size());

        Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        for (Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(f);
        }
        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            toMerge.addAll(returnedResult);
        }

        Collections.sort(toMerge, wrapperComparator);

        if (toMerge.size() > pagingPredicate.getPageSize()) {
            toMerge = toMerge.subList(0, pagingPredicate.getPageSize());
        }
        addQueryEntriesToResult(toMerge);
    }

    private static Collection<Collection<QueryableEntry>> getResult(List<Future<Collection<QueryableEntry>>> lsFutures) {
        return returnWithDeadline(lsFutures, QUERY_EXECUTION_TIMEOUT_MINUTES, TimeUnit.MINUTES, FutureUtil.RETHROW_EVERYTHING);
    }

    private boolean addQueryEntriesToResult(Collection<QueryableEntry> queryableEntries) {
        for (QueryableEntry entry : queryableEntries) {
            if (nodeResultLimitEnabled && ++resultSize > nodeResultLimit) {
                result.setResultLimitExceeded();
                return false;
            }
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
        }
        return true;
    }

    private void checkPartitionStateChanges(InternalPartitionService partitionService, int partitionStateVersion) {
        if (partitionStateVersion != partitionService.getPartitionStateVersion()) {
            getLogger().info("Partition assignments changed while executing query: " + predicate);
        }
    }

    @Override
    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeObject(predicate);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        predicate = in.readObject();
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
        }
    }

    private final class PartitionCallable implements Callable<Collection<QueryableEntry>> {

        private final int partition;

        private PartitionCallable(int partitionId) {
            this.partition = partitionId;
        }

        @Override
        public Collection<QueryableEntry> call() throws Exception {
            MapContextQuerySupport mapContextQuerySupport = mapService.getMapServiceContext().getMapContextQuerySupport();
            return mapContextQuerySupport.queryOnPartition(name, predicate, partition);
        }
    }
}
