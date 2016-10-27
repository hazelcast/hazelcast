/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.query;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.QueryOptimizer;
import com.hazelcast.util.executor.ManagedExecutorService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.map.impl.query.MapQueryEngineUtils.waitForResult;
import static com.hazelcast.query.PagingPredicateAccessor.getNearestAnchorEntry;
import static com.hazelcast.util.SortingUtil.getSortedSubList;

/**
 * Specialization of the {@link MapLocalQueryRunner} - the only difference is that
 * the query evaluation per partition is run in a PARALLEL fashion.
 * <p>
 * Runs query operations in the calling thread (thus blocking it)
 * Query evaluation per partition is run in a PARALLEL fashion.
 * <p>
 * Used by query operations only: QueryOperation & QueryPartitionOperation
 * Should not be used by proxies or any other query related objects.
 */
public class MapLocalParallelQueryRunner extends MapLocalQueryRunner {

    protected static final int QUERY_EXECUTION_TIMEOUT_MINUTES = 5;

    private final ManagedExecutorService executor;
    private final int timeoutInMinutes;

    public MapLocalParallelQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer,
                                       ManagedExecutorService executor, int timeoutInMinutes) {
        super(mapServiceContext, optimizer);
        this.executor = executor;
        this.timeoutInMinutes = timeoutInMinutes;
    }

    public MapLocalParallelQueryRunner(MapServiceContext mapServiceContext, QueryOptimizer optimizer,
                                       ManagedExecutorService executor) {
        this(mapServiceContext, optimizer, executor, QUERY_EXECUTION_TIMEOUT_MINUTES);
    }

    @Override
    protected Collection<QueryableEntry> runUsingPartitionScan(
            String mapName, Predicate predicate, Collection<Integer> partitions) {
        try {
            if (predicate instanceof PagingPredicate) {
                return runUsingPartitionScanWithPaging(mapName, (PagingPredicate) predicate, partitions);
            } else {
                return runUsingPartitionScanWithoutPaging(mapName, predicate, partitions);
            }
        } catch (InterruptedException e) {
            throw new HazelcastException(e.getMessage(), e);
        } catch (ExecutionException e) {
            throw new HazelcastException(e.getMessage(), e);
        }
    }

    protected List<QueryableEntry> runUsingPartitionScanWithPaging(
            String name, PagingPredicate predicate, Collection<Integer> partitions)
            throws InterruptedException, ExecutionException {

        List<QueryableEntry> result = runUsingPartitionScanWithoutPaging(name, predicate, partitions);
        Map.Entry<Integer, Map.Entry> nearestAnchorEntry = getNearestAnchorEntry(predicate);
        return getSortedSubList(result, predicate, nearestAnchorEntry);
    }

    protected List<QueryableEntry> runUsingPartitionScanWithoutPaging(
            String name, Predicate predicate, Collection<Integer> partitions)
            throws InterruptedException, ExecutionException {

        List<Future<Collection<QueryableEntry>>> futures = new ArrayList<Future<Collection<QueryableEntry>>>(partitions.size());

        for (Integer partitionId : partitions) {
            QueryPartitionCallable task = new QueryPartitionCallable(name, predicate, partitionId);
            Future<Collection<QueryableEntry>> future = executor.submit(task);
            futures.add(future);
        }

        Collection<Collection<QueryableEntry>> returnedResults = waitForResult(futures, timeoutInMinutes);
        List<QueryableEntry> result = new ArrayList<QueryableEntry>();
        for (Collection<QueryableEntry> returnedResult : returnedResults) {
            result.addAll(returnedResult);
        }
        return result;
    }

    private final class QueryPartitionCallable implements Callable<Collection<QueryableEntry>> {
        protected final int partition;
        protected final String name;
        protected final Predicate predicate;

        private QueryPartitionCallable(String name, Predicate predicate, int partitionId) {
            this.name = name;
            this.predicate = predicate;
            this.partition = partitionId;
        }

        @Override
        public Collection<QueryableEntry> call() throws Exception {
            return runUsingPartitionScanOnSinglePartition(name, predicate, partition);
        }
    }

}
