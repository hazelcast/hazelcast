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

package com.hazelcast.query.impl;

/**
 * Provides the query context support for partitioned indexes with enabled stats.
 */
public class PartitionQueryContextProviderWithStats implements QueryContextProvider {

    private final PartitionQueryContextWithStats queryContext;

    /**
     * Constructs a new partition query context provider with stats for the given
     * indexes.
     *
     * @param indexes the indexes to construct the new query context for.
     */
    public PartitionQueryContextProviderWithStats(Indexes indexes) {
        queryContext = new PartitionQueryContextWithStats(indexes);
    }

    @Override
    public QueryContext obtainContextFor(Indexes indexes, int ownedPartitionCount) {
        assert queryContext.ownedPartitionCount == 1 && ownedPartitionCount == 1;
        queryContext.attachTo(indexes, ownedPartitionCount);
        return queryContext;
    }

}
