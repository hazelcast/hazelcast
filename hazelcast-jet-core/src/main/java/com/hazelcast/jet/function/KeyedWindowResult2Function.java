/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.function;

import com.hazelcast.jet.pipeline.StageWithKeyAndWindow;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Represents the function you pass to a windowed group-and-aggregate
 * method in the Pipeline API, such as {@link StageWithKeyAndWindow#aggregate2
 * stage.aggregate2()}.
 * It creates the item to emit based on the results of a single aggregate
 * operation performed for a particular window and a particular grouping
 * key.
 * <p>
 * The parameters are:
 * <ol><li>
 *     {@code winStart} and {@code winEnd}: the starting and ending timestamp
 *     of the window (the end timestamp is the exclusive upper bound)
 * </li><li>
 *     {@code key} the grouping key
 * </li><li>
 *     {@code windowResult} the result of the aggregate operation
 * </li></ol>
 *
 * @param <K> type of the key
 * @param <R0> type of the aggregated result from stream-0
 * @param <R1> type of the aggregated result from stream-1
 * @param <OUT> the type of the output item this function returns
 */
@FunctionalInterface
public interface KeyedWindowResult2Function<K, R0, R1, OUT> extends Serializable {
    @Nonnull OUT apply(long winStart, long winEnd, @Nonnull K key, @Nonnull R0 result0, @Nonnull R1 result1);
}
