/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.query.PagingPredicate;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.Map;

/**
 * Result of a query. Each query type implements its own Result.
 *
 * @param <T> type of the result for the concrete implementations
 */
public interface Result<T extends Result> extends IdentifiedDataSerializable {

    /**
     * Returns partition IDs associated with this result or {@code null} if this
     * result is an empty/failure result.
     */
    PartitionIdSet getPartitionIds();

    /**
     * Sets the partition IDs of this result.
     *
     * @param partitionIds the partition IDs to set.
     */
    void setPartitionIds(PartitionIdSet partitionIds);

    /**
     * Combines the given result with this result modifying this result.
     *
     * @param result the result to combine with.
     * @see #onCombineFinished()
     */
    void combine(T result);

    /**
     * Invoked when the result combining phase is finished.
     * <p>
     * Implementations may release intermediary resources associated with
     * the combining. No further calls to {@link #combine} are expected after
     * this method invocation.
     *
     * @see #combine(Result)
     */
    void onCombineFinished();

    /**
     * Adds the given entry to this result.
     *
     * @param entry the entry to add.
     */
    void add(QueryableEntry entry);

    /**
     * Creates a new empty sub result of the same type as this result.
     * <p>
     * This method is used by the query execution engine while constructing the
     * partial sub results. These sub results are combined with this result
     * during the final stage of the result construction.
     *
     * @return the created sub result.
     */
    T createSubResult();

    /**
     * Performs the order-and-limit operation on this result for the given
     * paging predicate.
     *
     * @param pagingPredicate    the paging predicate to perform the operation
     *                           for.
     * @param nearestAnchorEntry the anchor entry of the paging predicate.
     */
    void orderAndLimit(PagingPredicate pagingPredicate, Map.Entry<Integer, Map.Entry> nearestAnchorEntry);

    /**
     * Completes the construction of this result.
     * <p>
     * Implementations may release intermediary resources associated with the
     * construction in this method. No further modifications of the result are
     * expected after this method invocation except combining the result with
     * other results.
     *
     * @param partitionIds the partition IDs to associate this result with.
     * @see #combine(Result)
     */
    void completeConstruction(PartitionIdSet partitionIds);

}
