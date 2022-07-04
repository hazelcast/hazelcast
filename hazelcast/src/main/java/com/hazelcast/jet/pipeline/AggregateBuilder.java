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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.CoAggregateOperationBuilder;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.AggBuilder;
import com.hazelcast.jet.impl.pipeline.AggBuilder.CreateOutStageFn;
import com.hazelcast.jet.impl.pipeline.BatchStageImpl;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.aggregate.AggregateOperations.coAggregateOperationBuilder;

/**
 * Offers a step-by-step API to build a pipeline stage that co-aggregates
 * the data from several input stages. To obtain it, call {@link
 * BatchStage#aggregateBuilder()} on the first stage you are co-aggregating
 * and refer to that method's Javadoc for further details.
 * <p>
 * <strong>Note:</strong> this is not a builder of {@code
 * AggregateOperation}. If that's what you are looking for, go {@link
 * AggregateOperation#withCreate here}.
 *
 * @param <R0> type of the result of the aggregate operation applied to stage-0
 *            (the one you obtained this builder from)
 *
 * @since Jet 3.0
 */
public class AggregateBuilder<R0> {
    private final AggBuilder aggBuilder;
    private final CoAggregateOperationBuilder aggrOpBuilder = coAggregateOperationBuilder();

    <T0> AggregateBuilder(
            @Nonnull BatchStage<T0> s,
            @Nonnull AggregateOperation1<? super T0, ?, ? extends R0> aggrOp
    ) {
        aggBuilder = new AggBuilder(s, null);
        aggrOpBuilder.add(Tag.tag0(), aggrOp);
    }

    /**
     * Returns the tag corresponding to the pipeline stage this builder was
     * obtained from. Use it to get the results for this stage from the
     * {@code ItemsByTag} appearing in the output of the stage you are building.
     */
    @Nonnull
    public Tag<R0> tag0() {
        return Tag.tag0();
    }

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to get the results
     * for this stage from the {@code ItemsByTag} appearing in the output of
     * the stage you are building.
     */
    @Nonnull
    public <T, R> Tag<R> add(
            @Nonnull BatchStage<T> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        Tag<T> tag = aggBuilder.add(stage);
        return aggrOpBuilder.add(tag, aggrOp);
    }

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag} and the {@code finishFn} you supply
     * transforms it to the final result to emit. Use the tags you got from
     * this builder in the implementation of {@code finishFn} to access the
     * results.
     *
     * @param finishFn the finishing function for the composite aggregate
     *     operation. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <R> the output item type
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <R> BatchStage<R> build(
            @Nonnull FunctionEx<? super ItemsByTag, ? extends R> finishFn
    ) {
        AggregateOperation<Object[], R> aggrOp = aggrOpBuilder.build(finishFn);
        CreateOutStageFn<R, BatchStage<R>> createOutStageFn = BatchStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn);
    }

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag}. Use the tags you got from
     * this builder to access the results.
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public BatchStage<ItemsByTag> build() {
        AggregateOperation<Object[], ItemsByTag> aggrOp = aggrOpBuilder.build();
        CreateOutStageFn<ItemsByTag, BatchStage<ItemsByTag>> createOutStageFn = BatchStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn);
    }
}
