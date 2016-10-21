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
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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

    static boolean shouldSkipPartitionsQuery(Collection<Integer> partitionIds) {
        return partitionIds == null || partitionIds.isEmpty();
    }

    static <T> Collection<Collection<T>> waitForResult(List<Future<Collection<T>>> lsFutures, int timeoutInMinutes) {
        return returnWithDeadline(lsFutures, timeoutInMinutes, MINUTES, RETHROW_EVERYTHING);
    }

    static Set<Integer> createSetWithPopulatedPartitionIds(int partitionCount) {
        Set<Integer> partitionIds = new HashSet<Integer>(partitionCount);
        for (int i = 0; i < partitionCount; i++) {
            partitionIds.add(i);
        }
        return partitionIds;
    }
}
