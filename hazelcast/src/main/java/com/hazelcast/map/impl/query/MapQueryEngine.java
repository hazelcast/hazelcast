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

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.IterationType;

import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Responsible for executing queries on the IMap.
 */
public interface MapQueryEngine {

    /**
     * Executes a query on all the local partitions.
     * <p>
     * - Uses Indexes
     * - Accepts PagingPredicate
     * - Query executed in the calling thread
     * - predicate evaluation will be parallelized if QUERY_PREDICATE_PARALLEL_EVALUATION is enabled or a PagingPredicate is used.
     * - may return empty QueryResult (with a properly initialized result limit size) if query is executed during migrations and
     * migration conditions do not allow for safe retrieval of results (ie. there are migrations in flight or partition state
     * version is different at the end of query vs the version observed at the beginning of query). In this case, partitionIds
     * are not set on the QueryResult, so callers will ignore these results and fallback to {@code QueryPartitionOperation}s.
     *
     * @param mapName   the name of the map
     * @param predicate the predicate
     * @return the QueryResult
     * @throws ExecutionException
     * @throws InterruptedException
     */
    QueryResult queryLocalPartitions(String mapName, Predicate predicate, IterationType iterationType)
            throws ExecutionException, InterruptedException;

    /**
     * Executes a query on a specific local partition.
     * <p>
     * - Does NOT use Indexes
     * - Accepts PagingPredicate
     * - Sequential full table scan
     * - Query executed in the calling thread
     * <p>
     * todo: what happens when the partition is not local?
     *
     * @param mapName     map name.
     * @param predicate   any predicate.
     * @param partitionId partition id.
     * @return result of query
     */
    QueryResult queryLocalPartition(String mapName, Predicate predicate, int partitionId, IterationType iterationType);

    /**
     * Query all local partitions.
     * <p>
     * - Does NOT accept PagingPredicate
     * - Query executed in an Operation on this member (NOT in the calling thread)
     * - Calls {@link #queryLocalPartitions(String, Predicate, IterationType)} in an operation
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult invokeQueryLocalPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Queries all partitions. Paging predicates are not allowed.
     * - Does NOT accept PagingPredicate
     * - Query executed in an Operation on each member (NOT in the calling thread)
     * - Calls {@link #queryLocalPartitions(String, Predicate, IterationType)} in an operation
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult invokeQueryAllPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Query all local partitions with a paging predicate.
     * <p>
     * - Query executed in an Operation on this member (or other members if some partitions are not local)
     * <p>
     * TODO:
     * it would be better to have a single queryLocal... method and let the implementation figure out how to deal
     * with a regular predicate and a paging predicate. No need to have that in the interface. The problem is that currently
     * the signatures don't match up. This implementation detail should not be exposed through the interface.
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    Set queryLocalPartitionsWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType);

    /**
     * Queries all partitions with a paging predicate.
     * <p>
     * - Query executed in an Operation on each member (NOT in the calling thread)
     * <p>
     * TODO:
     * it would be better to have single queryAll method and let the implementation figure out how to deal
     * with a paging predicate. See comment in {@link #queryLocalPartitionsWithPagingPredicate}
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    Set queryAllPartitionsWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType);
}
