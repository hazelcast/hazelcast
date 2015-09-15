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
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.Set;

/**
 * Components responsible for executing queries.
 */
public interface MapQueryEngine {

    /**
     * Query a specific partition.
     *
     * @param mapName     map name.
     * @param predicate   any predicate.
     * @param partitionId partition id.
     * @return result of query
     */
    Collection<QueryableEntry> queryOnPartition(String mapName, Predicate predicate, int partitionId);

    /**
     * Used for predicates which queries on node local entries, except paging predicate.
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link com.hazelcast.util.IterationType}
     * @param binary        <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return the results
     */
    Set queryLocalMember(String mapName, Predicate predicate,
                         IterationType iterationType, boolean binary);

    /**
     * Used for paging predicate queries on node local entries.
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return the results
     */
    Set queryLocalMemberWithPagingPredicate(String mapName, PagingPredicate pagingPredicate,
                                            IterationType iterationType);

    /**
     * Used for paging predicate queries on all members.
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return the results
     */
    Set queryWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType);

    /**
     * Used for predicates which queries on all members, except paging predicate.
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link IterationType}
     * @param binary        <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return the results
     */
    Collection query(String mapName, Predicate predicate,
                     IterationType iterationType, boolean binary, boolean unique);

    /**
     * Creates a {@link QueryResult} with configured result limit (according to the number of partitions) if feature is enabled.
     *
     * @param numberOfPartitions number of partitions to calculate result limit
     * @return results
     */
    QueryResult newQueryResult(IterationType iterationType, int numberOfPartitions);
}
