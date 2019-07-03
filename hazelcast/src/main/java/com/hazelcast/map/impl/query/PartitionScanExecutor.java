/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntriesSegment;

import java.util.Collection;

/**
 * Responsible for executing a full partition scan for the given partitions.
 * May execute the scan in the calling thread or delegate to other thread - it depends on the implementation.
 */
public interface PartitionScanExecutor {

    void execute(String mapName, Predicate predicate, Collection<Integer> partitions, Result result);

    /**
     * Executes the predicate on a partition chunk. The offset in the partition is defined by the {@code tableIndex}
     * and the soft limit is defined by the {@code fetchSize}. The method returns the matched entries and an
     * index from which new entries can be fetched which allows for efficient iteration of query results.
     * <p>
     * <b>NOTE</b>
     * Iterating the query results using the returned next table index should be done
     * only when the {@link IMap} is not being mutated and the cluster is
     * stable (there are no migrations or membership changes).
     * In other cases, entries are rearranged and the you may get the same query result twice or
     * may miss some query results that match the predicate.
     *
     * @param mapName     the map name
     * @param predicate   the predicate which the entries must match
     * @param partitionId the partition which is queried
     * @param tableIndex  the index from which entries are queried
     * @param fetchSize   the soft limit for the number of entries to fetch
     * @return entries matching the predicate and a table index from which new entries can be fetched
     */
    QueryableEntriesSegment execute(String mapName, Predicate predicate, int partitionId, int tableIndex, int fetchSize);
}
