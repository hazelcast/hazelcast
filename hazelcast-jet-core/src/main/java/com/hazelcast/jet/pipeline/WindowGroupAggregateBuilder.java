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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.KeyedWindowResultFunction;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

import javax.annotation.Nonnull;

/**
 * Offers a step-by-step fluent API to build a pipeline stage that
 * performs a windowed co-grouping and aggregation of the data from several
 * input stages. To obtain it, call {@link
 * StageWithGroupingAndWindow#aggregateBuilder()} on one of the stages to
 * co-aggregate, then add the other stages by calling {@link #add
 * add(stage)} on the builder. Collect all the tags returned from {@code
 * add()} and use them when building the aggregate operation. Retrieve the
 * tag of the first stage (from which you obtained the builder) by calling
 * {@link #tag0()}.
 * <p>
 * This object is mainly intended to build a co-aggregation of four or more
 * contributing stages. For up to three stages, prefer the direct {@code
 * stage.aggregateN(...)} calls because they offer more static type safety.
 *
 * @param <T0> the type of the stream-0 item
 */
public class WindowGroupAggregateBuilder<T0, K> {
    private final GrAggBuilder<K> graggBuilder;

    public WindowGroupAggregateBuilder(StageWithGroupingAndWindow<T0, K> s) {
        graggBuilder = new GrAggBuilder<>(s);
    }

    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    @SuppressWarnings("unchecked")
    public <E> Tag<E> add(StreamStageWithGrouping<E, K> stage) {
        return graggBuilder.add(stage);
    }

    public <A, R, OUT> StreamStage<OUT> build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return graggBuilder.buildStream(aggrOp, mapToOutputFn);
    }

    public <A, R> StreamStage<TimestampedEntry<K, R>> build(
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        return graggBuilder.buildStream(aggrOp, TimestampedEntry::new);
    }
}
