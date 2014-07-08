package com.hazelcast.map;

import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.IterationType;

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
     * @return {@link com.hazelcast.map.QueryResult}
     */
    QueryResult queryOnPartition(String mapName, Predicate predicate, int partitionId);

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
}
