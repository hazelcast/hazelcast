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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.AggBuilder;
import com.hazelcast.jet.impl.pipeline.AggBuilder.CreateOutStageFn;
import com.hazelcast.jet.impl.pipeline.BatchStageImpl;

import javax.annotation.Nonnull;

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
 * @param <T0> type of items in stage-0 (the one you obtained this builder from)
 *
 * @since Jet 3.0
 */
public class AggregateBuilder1<T0> {
    private final AggBuilder aggBuilder;

    AggregateBuilder1(@Nonnull BatchStage<T0> stage) {
        this.aggBuilder = new AggBuilder(stage, null);
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
    public <T> Tag<T> add(@Nonnull BatchStage<T> stage) {
        return aggBuilder.add(stage);
    }

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of pipeline stages registered with this builder object. The tags you
     * register with the aggregate operation must match the tags you registered
     * with this builder. Refer to the documentation on {@link
     * BatchStage#aggregateBuilder()} for more details.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the output item
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <R> BatchStage<R> build(@Nonnull AggregateOperation<?, R> aggrOp) {
        CreateOutStageFn<R, BatchStage<R>> createOutStageFn = BatchStageImpl::new;
        return aggBuilder.build(aggrOp, createOutStageFn);
    }
}
