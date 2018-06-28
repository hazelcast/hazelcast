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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

/**
 * Represents the function you pass to a windowed aggregation method in the
 * Pipeline API, such as {@link com.hazelcast.jet.pipeline.StageWithWindow#aggregate2
 * stage.aggregate2()}, that aggregates two input streams. It creates the
 * item to emit based on the two results of the aggregate operation
 * performed for a particular window.
 * <p>
 * The parameters are:
 * <ol><li>
 *     {@code winStart} and {@code winEnd}: the starting and ending timestamp
 *     of the window (the end timestamp is the exclusive upper bound)
 * </li><li>
 *     {@code result0} aggregated result of input stream-0
 * </li><li>
 *     {@code result1} aggregated result of input stream-1
 * </li></ol>
 *
 * @param <R0> type of the aggregated result from stream-0
 * @param <R1> type of the aggregated result from stream-1
 * @param <R2> type of the aggregated result from stream-2
 * @param <OUT> the type of the output item this function returns
 */
@FunctionalInterface
public interface WindowResult3Function<R0, R1, R2, OUT> extends Serializable {

    @Nullable
    OUT apply(long start, long end, @Nonnull R0 result0, @Nonnull R1 result1, @Nonnull R2 result2);

    default KeyedWindowResult3Function<Object, R0, R1, R2, OUT> toKeyedWindowResult3Fn() {
        return (start, end, k, result0, result1, result2) -> apply(start, end, result0, result1, result2);
    }
}
