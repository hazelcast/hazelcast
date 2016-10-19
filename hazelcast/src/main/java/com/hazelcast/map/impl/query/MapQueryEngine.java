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

/**
 * Responsible for executing queries on the IMap.
 */
public interface MapQueryEngine {

    /**
     * Query all local partitions.
     * <p>
     * - Does NOT accept PagingPredicate
     * - Query executed in an Operation on this member (NOT in the calling thread)
     * - Calls {@link #runLocalFullQuery(String, Predicate, IterationType)} in an operation
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult runQueryOnLocalPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Queries a single partition. Paging predicates are not allowed.
     *
     * @param mapName mape name
     * @param predicate except paging predicate
     * @param iterationType the IterationType
     * @param partitionId the id of the partition
     * @return the result
     */
    QueryResult runQueryOnSinglePartition(String mapName, Predicate predicate, IterationType iterationType, int partitionId);

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
     * @param mapName       map name.
     * @param predicate     to queryOnMembers.
     * @param iterationType type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    Set runQueryOnLocalPartitionsWithPagingPredicate(String mapName, PagingPredicate predicate, IterationType iterationType);

    /**
     * Queries all partitions. Paging predicates are not allowed.
     * - Does NOT accept PagingPredicate
     * - Query executed in an Operation on each member (NOT in the calling thread)
     * - Calls {@link #runLocalFullQuery(String, Predicate, IterationType)} in an operation
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType the IterationType
     */
    QueryResult runQueryOnAllPartitions(String mapName, Predicate predicate, IterationType iterationType);

    /**
     * Queries all partitions with a paging predicate.
     * <p>
     * - Query executed in an Operation on each member (NOT in the calling thread)
     * <p>
     * TODO:
     * it would be better to have single queryAll method and let the implementation figure out how to deal
     * with a paging predicate. See comment in {@link #runQueryOnLocalPartitionsWithPagingPredicate}
     *
     * @param mapName         map name.
     * @param predicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link SortedQueryResultSet}
     */
    Set runQueryOnAllPartitionsWithPagingPredicate(String mapName, PagingPredicate predicate, IterationType iterationType);
}
