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

package com.hazelcast.map.operation;

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.map.MapContextQuerySupport;
import com.hazelcast.map.MapService;
import com.hazelcast.map.MapServiceContext;
import com.hazelcast.map.QueryResult;
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

    Predicate predicate;
    QueryResult result;
    PagingPredicate pagingPredicate;

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
        }
    }

    public QueryOperation() {
    }

    public void run() throws Exception {
        NodeEngine nodeEngine = getNodeEngine();
        InternalPartitionService partitionService = nodeEngine.getPartitionService();
        Collection<Integer> initialPartitions = mapService.getMapServiceContext().getOwnedPartitions();
        IndexService indexService = mapService.getMapServiceContext().getMapContainer(name).getIndexService();
        Set<QueryableEntry> entries = null;
        // TODO: fix
        if (!partitionService.hasOnGoingMigration()) {
            entries = indexService.query(predicate);
        }
        result = new QueryResult();
        if (entries != null) {
            for (QueryableEntry entry : entries) {
                result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
            }
        } else {
            // run in parallel
            if (pagingPredicate != null) {
                runParallelForPaging(initialPartitions);
            } else {
                runParallel(initialPartitions);
            }
        }
        Collection<Integer> finalPartitions = mapService.getMapServiceContext().getOwnedPartitions();
        if (initialPartitions.equals(finalPartitions)) {
            result.setPartitionIds(finalPartitions);
        }
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            final MapServiceContext mapServiceContext = ((MapService) getService())
                    .getMapServiceContext();
            mapServiceContext
                    .getLocalMapStatsProvider().getLocalMapStatsImpl(name).incrementOtherOperations();
        }
    }

    protected void runParallel(final Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final ExecutorService executor
                = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        final List<Future<Collection<QueryableEntry>>> lsFutures
                = new ArrayList<Future<Collection<QueryableEntry>>>(initialPartitions.size());
        for (Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(f);
        }

        final Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            if (returnedResult == null) {
                continue;
            }
            for (QueryableEntry entry : returnedResult) {
                result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
            }
        }
    }

    private static Collection<Collection<QueryableEntry>> getResult(List<Future<Collection<QueryableEntry>>> lsFutures) {
        return returnWithDeadline(lsFutures,
                QUERY_EXECUTION_TIMEOUT_MINUTES, TimeUnit.MINUTES, FutureUtil.RETHROW_EVERYTHING);
    }

    protected void runParallelForPaging(Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        final NodeEngine nodeEngine = getNodeEngine();
        final ExecutorService executor
                = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        final List<Future<Collection<QueryableEntry>>> lsFutures
                = new ArrayList<Future<Collection<QueryableEntry>>>(initialPartitions.size());

        final Comparator<Map.Entry> wrapperComparator = SortingUtil.newComparator(pagingPredicate);
        for (final Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> f = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(f);
        }
        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        final Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            toMerge.addAll(returnedResult);
        }

        Collections.sort(toMerge, wrapperComparator);

        if (toMerge.size() > pagingPredicate.getPageSize()) {
            toMerge = toMerge.subList(0, pagingPredicate.getPageSize());
        }
        for (QueryableEntry entry : toMerge) {
            result.add(new QueryResultEntryImpl(entry.getKeyData(), entry.getKeyData(), entry.getValueData()));
        }
    }

    public ExceptionAction onException(Throwable throwable) {
        if (throwable instanceof MemberLeftException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        if (throwable instanceof TargetNotMemberException) {
            return ExceptionAction.THROW_EXCEPTION;
        }
        return super.onException(throwable);
    }

    public Object getResponse() {
        return result;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(name);
        out.writeObject(predicate);
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        name = in.readUTF();
        predicate = in.readObject();
        if (predicate instanceof PagingPredicate) {
            pagingPredicate = (PagingPredicate) predicate;
        }
    }

    private final class PartitionCallable implements Callable<Collection<QueryableEntry>> {

        final int partition;

        private PartitionCallable(int partitionId) {
            this.partition = partitionId;
        }

        public Collection<QueryableEntry> call() throws Exception {
            MapContextQuerySupport mapContextQuerySupport = mapService.getMapServiceContext()
                    .getMapContextQuerySupport();
            return mapContextQuerySupport.queryOnPartition(name, predicate, partition);
        }
    }

}
