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
 * Provides query contexts for {@link Indexes} to execute queries.
 */
public interface QueryContextProvider {

    /**
     * Obtains a query context for the given indexes.
     * <p>
     * The returned query context instance is valid for a duration of a single
     * query and should be re-obtained for every query.
     *
     * @param indexes             the indexes to obtain the query context for.
     * @param ownedPartitionCount a count of owned partitions a query runs on.
     *                            Negative value indicates that the value is not defined.
     * @return the obtained query context.
     */
    QueryContext obtainContextFor(Indexes indexes, int ownedPartitionCount);

}
