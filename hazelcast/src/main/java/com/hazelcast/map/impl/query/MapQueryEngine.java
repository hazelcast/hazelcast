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
     *
     * @param name      the name of the map
     * @param predicate the predicate
     * @return the QueryResult
     * @throws ExecutionException
     * @throws InterruptedException
     */
    QueryResult queryLocalPartitions(String name, Predicate predicate, IterationType iterationType)
            throws ExecutionException, InterruptedException;

    /**
     * Executes a query a specific local partition.
     *
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
     *
     * todo: we need better explanation of difference between this method and
     *  {@link #queryLocalPartitions(String, Predicate, IterationType)}
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult invokeQueryLocalPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Queries all partitions. Paging predicates are not allowed.
     *
     * @param mapName   map name.
     * @param predicate except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult invokeQueryAllPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Query all local partitions with a paging predicate.
     *
     * todo: it would be better to have a single queryLocal... method and let the implementation figure out how to deal
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
     *
     * todo: it would be better to have single queryAll method and let the implementation figure out how to deal
     * with a paging predicate. See comment in {@link #queryLocalPartitionsWithPagingPredicate}
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    Set queryAllPartitionsWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType);
}
