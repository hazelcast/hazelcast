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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;

/**
 * Offers a step-by-step API to build a pipeline stage that co-groups and
 * aggregates the data from several input stages. To obtain it, call {@link
 * BatchStageWithKey#aggregateBuilder(AggregateOperation1)
 * stage.aggregateBuilder(aggrOp)} on one of the stages to co-group and
 * refer to that method's Javadoc for further details.
 * <p>
 * <strong>Note:</strong> this is not a builder of {@code
 * AggregateOperation}. If that' s what you are looking for, go {@link
 * AggregateOperation#withCreate here}.
 *
 * @param <K> type of the grouping key
 * @param <R0> type of the aggregation result for stream-0
 *
 * @since Jet 3.0
 */
public class GroupAggregateBuilder<K, R0> {
    private final GrAggBuilder<K> grAggBuilder;
    private final CoAggregateOperationBuilder aggrOpBuilder = coAggregateOperationBuilder();

    <T0> GroupAggregateBuilder(
            @Nonnull BatchStageWithKey<T0, K> stage0,
            @Nonnull AggregateOperation1<? super T0, ?, ? extends R0> aggrOp0
    ) {
        grAggBuilder = new GrAggBuilder<>(stage0);
        aggrOpBuilder.add(Tag.tag0(), aggrOp0);
    }

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<R0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    public <T, R> Tag<R> add(
            @Nonnull BatchStageWithKey<T, K> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        Tag<T> tag = grAggBuilder.add(stage);
        return aggrOpBuilder.add(tag, aggrOp);
    }

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object and emits a {@code
     * Map.Entry(key, resultsByTag)} for each distinct key. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag}. Use the tags you got from this
     * builder to access the results.
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public BatchStage<Entry<K, ItemsByTag>> build() {
        AggregateOperation<Object[], ItemsByTag> aggrOp = aggrOpBuilder.build();
        return grAggBuilder.buildBatch(aggrOp, Util::entry);
    }
}
