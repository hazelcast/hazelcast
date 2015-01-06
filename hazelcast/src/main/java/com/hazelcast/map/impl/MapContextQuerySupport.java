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

package com.hazelcast.map.impl;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.util.IterationType;

import java.util.Collection;
import java.util.Set;

/**
 * Support interface which is used in map specific query operations.
 */
public interface MapContextQuerySupport {

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
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link com.hazelcast.util.QueryResultSet}
     */
    Set queryLocalMember(String mapName, Predicate predicate,
                         IterationType iterationType, boolean dataResult);

    /**
     * Used for paging predicate queries on node local entries.
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link com.hazelcast.util.SortedQueryResultSet}
     */
    Set queryLocalMemberWithPagingPredicate(String mapName, PagingPredicate pagingPredicate,
                                            IterationType iterationType);

    /**
     * Used for paging predicate queries on all members.
     *
     * @param mapName         map name.
     * @param pagingPredicate to queryOnMembers.
     * @param iterationType   type of {@link IterationType}
     * @return {@link com.hazelcast.util.SortedQueryResultSet}
     */
    Set queryWithPagingPredicate(String mapName, PagingPredicate pagingPredicate, IterationType iterationType);

    /**
     * Used for predicates which queries on all members, except paging predicate.
     *
     * @param mapName       map name.
     * @param predicate     except paging predicate.
     * @param iterationType type of {@link IterationType}
     * @param dataResult    <code>true</code> if results should contain {@link com.hazelcast.nio.serialization.Data} types,
     *                      <code>false</code> for object types.
     * @return {@link com.hazelcast.util.QueryResultSet}
     */
    Set query(String mapName, Predicate predicate,
              IterationType iterationType, boolean dataResult);

    /**
     * Checks if the query result limit feature is enabled.
     *
     * @return <tt>true</tt> if the query result size limitation is enabled, <tt>false</tt> otherwise
     */
    boolean isQueryResultLimitEnabled();

    /**
     * Calculates the query result limit for a node.
     *
     * @param ownedPartitions number of owned partitions by the node
     * @return result size limit
     */
    long getNodeResultLimit(int ownedPartitions);
}
