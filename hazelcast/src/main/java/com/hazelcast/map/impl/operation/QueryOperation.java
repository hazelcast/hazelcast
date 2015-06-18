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
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.map.impl.MapContextQuerySupport;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.QueryResult;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.InternalPartitionService;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.util.FutureUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

public class QueryOperation extends AbstractMapOperation implements ReadonlyOperation {

    private static final long QUERY_EXECUTION_TIMEOUT_MINUTES = 5;

    private Predicate predicate;
    private PagingPredicate pagingPredicate;

    private QueryResult result;

    public QueryOperation() {
    }

    public QueryOperation(String mapName, Predicate predicate) {
        super(mapName);
        this.predicate = predicate;
        if (predicate instanceof PagingPredicate) {
            this.pagingPredicate = (PagingPredicate) predicate;
        }
    }

    private static Collection<Collection<QueryableEntry>> getResult(List<Future<Collection<QueryableEntry>>> lsFutures) {
        return returnWithDeadline(lsFutures, QUERY_EXECUTION_TIMEOUT_MINUTES, TimeUnit.MINUTES, FutureUtil.RETHROW_EVERYTHING);
    }

    @Override
    public void run() throws Exception {
        InternalPartitionService partitionService = getNodeEngine().getPartitionService();
        NodeEngine nodeEngine = getNodeEngine();
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContextQuerySupport mapQuerySupport = mapServiceContext.getMapContextQuerySupport();

        int partitionStateVersion = partitionService.getPartitionStateVersion();
        Collection<Integer> initialPartitions = mapServiceContext.getOwnedPartitions();

        Set<QueryableEntry> entries = null;
        if (!partitionService.hasOnGoingMigrationLocal()) {
            entries = mapContainer.getIndexService().query(predicate);
        }

        result = mapQuerySupport.newQueryResult(initialPartitions.size());
        if (entries != null) {
            result.addAll(entries);
        } else {
            fullTableScan(initialPartitions, nodeEngine.getGroupProperties());
        }
        Collection<Integer> finalPartitions = mapServiceContext.getOwnedPartitions();
        if (initialPartitions.equals(finalPartitions)) {
            result.setPartitionIds(finalPartitions);
        }
        if (mapContainer.getMapConfig().isStatisticsEnabled()) {
            LocalMapStatsImpl localStats = mapServiceContext.getLocalMapStatsProvider().getLocalMapStatsImpl(name);
            localStats.incrementOtherOperations();
        }

        checkPartitionStateChanges(partitionService, partitionStateVersion);
    }

    private void fullTableScan(Collection<Integer> initialPartitions, GroupProperties groupProperties)
            throws InterruptedException, ExecutionException {
        if (pagingPredicate != null) {
            runParallelForPaging(initialPartitions);
        } else {
            boolean parallelEvaluation = groupProperties.QUERY_PREDICATE_PARALLEL_EVALUATION.getBoolean();
            if (parallelEvaluation) {
                runParallel(initialPartitions);
            } else {
                runSingleThreaded(initialPartitions);
            }
        }
    }

    protected void runSingleThreaded(final Collection<Integer> initialPartitions) {
        RetryableHazelcastException storedException = null;
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapContextQuerySupport querySupport = mapServiceContext.getMapContextQuerySupport();
        for (Integer partitionId : initialPartitions) {
            try {
                Collection<QueryableEntry> entries = querySupport.queryOnPartition(name, predicate, partitionId);
                result.addAll(entries);
            } catch (RetryableHazelcastException e) {
                // RetryableHazelcastException are stored and re-thrown later. this is to ensure all partitions
                // are touched as when the parallel execution was used.
                // see discussion at https://github.com/hazelcast/hazelcast/pull/5049#discussion_r28773099 for details.
                if (storedException == null) {
                    storedException = e;
                }
            }
        }
        if (storedException != null) {
            throw storedException;
        }
    }

    private void runParallel(Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(
                initialPartitions.size());

        for (Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> future = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(future);
        }

        Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            if (returnedResult == null) {
                continue;
            }
            result.addAll(returnedResult);
        }
    }

    private void runParallelForPaging(Collection<Integer> initialPartitions) throws InterruptedException, ExecutionException {
        NodeEngine nodeEngine = getNodeEngine();
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(ExecutionService.QUERY_EXECUTOR);
        List<Future<Collection<QueryableEntry>>> lsFutures = new ArrayList<Future<Collection<QueryableEntry>>>(
                initialPartitions.size());

        for (Integer partitionId : initialPartitions) {
            Future<Collection<QueryableEntry>> future = executor.submit(new PartitionCallable(partitionId));
            lsFutures.add(future);
        }
        List<QueryableEntry> toMerge = new LinkedList<QueryableEntry>();
        Collection<Collection<QueryableEntry>> returnedResults = getResult(lsFutures);
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            toMerge.addAll(returnedResult);
        }

        List<QueryableEntry> sortedSubList = getSortedSubList(toMerge, pagingPredicate);
        result.addAll(sortedSubList);
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
