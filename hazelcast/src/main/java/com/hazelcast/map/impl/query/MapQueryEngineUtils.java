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

import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.IterationType;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.util.FutureUtil.returnWithDeadline;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MINUTES;

final class MapQueryEngineUtils {

    private MapQueryEngineUtils() {
    }

    static QueryResult newQueryResult(int numberOfPartitions, IterationType iterationType,
                                      QueryResultSizeLimiter resultSizeLimiter) {
        return new QueryResult(iterationType, resultSizeLimiter.getNodeResultLimit(numberOfPartitions));
    }

    static QueryResult newQueryResultForSinglePartition(
            Collection<QueryableEntry> queryableEntries, int partitionId, IterationType iterationType,
            QueryResultSizeLimiter queryResultSizeLimiter) {
        QueryResult result = newQueryResult(1, iterationType, queryResultSizeLimiter);
        result.addAll(queryableEntries);
        result.setPartitionIds(singletonList(partitionId));
        return result;
    }


    static Object toObject(SerializationService ss, Object obj) {
        return ss.toObject(obj);
    }

    /**
     * Adds results of paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    static void addResultsOfPagingPredicate(
            SerializationService ss, List<Future<QueryResult>> futures, Collection result, Collection<Integer> partitionIds)
            throws ExecutionException, InterruptedException {
        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
            if (queryResult == null) {
                continue;
            }

            Collection<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                if (!partitionIds.containsAll(queriedPartitionIds)) {
                    // results for at least one partition have already been added, so discard these results
                    // see https://github.com/hazelcast/hazelcast/issues/6471
                    continue;
                }
                partitionIds.removeAll(queriedPartitionIds);
                for (QueryResultRow row : queryResult.getRows()) {
                    Object key = toObject(ss, row.getKey());
                    Object value = toObject(ss, row.getValue());
                    result.add(new AbstractMap.SimpleImmutableEntry<Object, Object>(key, value));
                }
            }
        }
    }

    /**
     * Adds results of non-paging predicates to result set and removes queried partition ids.
     */
    @SuppressWarnings("unchecked")
    static void addResultsOfPredicate(List<Future<QueryResult>> futures, QueryResult result,
                                      Collection<Integer> partitionIds) throws ExecutionException, InterruptedException {

        for (Future<QueryResult> future : futures) {
            QueryResult queryResult = future.get();
            if (queryResult == null) {
                continue;
            }
            Collection<Integer> queriedPartitionIds = queryResult.getPartitionIds();
            if (queriedPartitionIds != null) {
                if (!partitionIds.containsAll(queriedPartitionIds)) {
                    // do not take into account results that contain partition IDs already removed from partitionIds
                    // collection as this means that we will count results from a single partition twice
                    // see also https://github.com/hazelcast/hazelcast/issues/6471
                    continue;
                }
                partitionIds.removeAll(queriedPartitionIds);
                result.addAllRows(queryResult.getRows());
            }
        }
    }

    static boolean shouldSkipPartitionsQuery(Collection<Integer> partitionIds) {
        return partitionIds == null || partitionIds.isEmpty();
    }

    static <T> Collection<Collection<T>> waitForResult(List<Future<Collection<T>>> lsFutures, int timeoutInMinutes) {
        return returnWithDeadline(lsFutures, timeoutInMinutes, MINUTES, RETHROW_EVERYTHING);
    }

}
