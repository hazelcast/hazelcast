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

import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Offers a step-by-step API to build a pipeline stage that co-groups and
 * aggregates the data from several input stages. To obtain it, call {@link
 * StageWithGrouping#aggregateBuilder()} on one of the stages to co-group
 * and refer to that method's Javadoc for further details.
 *
 * @param <T0> type of the stream-0 item
 * @param <K> type of the grouping key
 */
public class GroupAggregateBuilder1<T0, K> {
    private final GrAggBuilder<K> grAggBuilder;

    GroupAggregateBuilder1(@Nonnull StageWithGrouping<T0, K> s) {
        grAggBuilder = new GrAggBuilder<>(s);
    }

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<T0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    @SuppressWarnings("unchecked")
    public <T> Tag<T> add(@Nonnull StageWithGrouping<T, K> stage) {
        return grAggBuilder.add(stage);
    }

    /**
     * Creates and returns a pipeline stage that performs the
     * co-grouping and aggregation of pipeline stages registered with this
     * builder object. The tags you register with the aggregate operation must
     * match the tags you registered with this builder.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <A> the type of items on the stage this builder was obtained from
     * @param <R> the type of the output item
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <A, R, OUT> BatchStage<OUT> build(
            @Nonnull AggregateOperation<A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn
    ) {
        return grAggBuilder.buildBatch(aggrOp, mapToOutputFn);
    }

    /**
     * Convenience for {@link #build(AggregateOperation, DistributedBiFunction)
     * build(aggrOp, mapToOutputFn)} which emits {@code Map.Entry}s as output.
     *
     * @param aggrOp the aggregate operation to perform.
     * @param <A> the type of items in the pipeline stage this builder was obtained from
     * @param <R> the type of the aggregation result
     * @return a new stage representing the co-group-and-aggregate operation
     */
    @Nonnull
    public <A, R> BatchStage<Entry<K, R>> build(
            @Nonnull AggregateOperation<A, R> aggrOp
    ) {
        return grAggBuilder.buildBatch(aggrOp, Util::entry);
    }
}
