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

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.query.impl.QueryableEntry;

/**
 * Responsible for creating query results of a concrete type depending on the type of the Query.
 * The populated query result will contain the given entries in a processed or un-processed way.
 *
 * @param <T> type of the result
 */
public interface ResultProcessor<T extends Result> {

    /**
     * Populate an empty result for the given query type.
     *
     * @param query       given query
     * @param resultLimit given resultLimit (ignored)
     * @return Result of a concrete type.
     */
    T populateResult(Query query, long resultLimit);

    /**
     * Populate an non-empty result for the given query type.
     *
     * @param query        given query
     * @param resultLimit  given resultLimit
     * @param entries      entries to add to the result to
     * @param partitionIds partitionIds where the given entries reside
     * @return Result of a concrete type.
     */
    T populateResult(Query query, long resultLimit, Iterable<QueryableEntry> entries, PartitionIdSet partitionIds);
}
