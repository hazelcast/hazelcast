/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.datastream;

import com.hazelcast.query.Predicate;

/**
 * https://pandas.pydata.org/pandas-docs/stable/reference/frame.html
 *
 * @param <R>
 */
public interface DataFrame<R> {

    PreparedEntryProcessor prepare(EntryProcessorRecipe recipe);

    PreparedQuery<R> prepare(Predicate predicate);

    <E> PreparedProjection<E> prepare(ProjectionRecipe<E> recipe);

    <T, E> PreparedAggregation<E> prepare(AggregationRecipe<T, E> recipe);

    <E> E aggregate(String aggregatorId);

    /**
     * No more mutations (important to close the eden region so no processing on partition thread)
     */
    void freeze();

    /**
     * Returns the number of records in this set.
     *
     * @return number of items in this set.
     */
    long count();

    DataStreamInfo memoryInfo();

    DataStreamInfo memoryInfo(int partitionId);

    LongDataSeries getLongDataSeries(String field);
}
