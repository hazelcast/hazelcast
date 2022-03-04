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
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.impl.pipeline.GrAggBuilder;

import javax.annotation.Nonnull;

/**
 * Offers a step-by-step API to build a pipeline stage that performs a
 * windowed co-grouping and aggregation of the data from several input
 * stages. To obtain it, call {@link StageWithKeyAndWindow#aggregateBuilder()}
 * on one of the stages to co-aggregate and refer to that method's Javadoc
 * for further details.
 * <p>
 * This object is mainly intended to build a co-aggregation of four or more
 * contributing stages. For up to three stages, prefer the direct {@code
 * stage.aggregateN(...)} calls because they offer more static type safety.
 * <p>
 * <strong>Note:</strong> this is not a builder of {@code
 * AggregateOperation}. If that' s what you are looking for, go {@link
 * AggregateOperation#withCreate here}.
 *
 * @param <T0> the type of the stream-0 item
 * @param <K> type of the key
 *
 * @since Jet 3.0
 */
public class WindowGroupAggregateBuilder1<T0, K> {
    private final GrAggBuilder<K> grAggBuilder;

    WindowGroupAggregateBuilder1(@Nonnull StageWithKeyAndWindow<T0, K> s) {
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
     * Adds another stage that will contribute its data to the windowed
     * group-and-aggregate stage being constructed. Returns the tag you'll use
     * to refer to this stage when building the {@code AggregateOperation} that
     * you'll pass to {@link #build build()}.
     */
    @Nonnull
    public <T> Tag<T> add(@Nonnull StreamStageWithKey<T, K> stage) {
        return grAggBuilder.add(stage);
    }

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * cogroup-and-aggregate of the pipeline stages registered with this builder object.
     * The tags you register with the aggregate operation must match the tags
     * you registered with this builder.
     *
     * @see com.hazelcast.jet.aggregate.AggregateOperations AggregateOperations
     * @param aggrOp        the aggregate operation to perform
     * @param <R>           the type of the aggregation result
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public <R> StreamStage<KeyedWindowResult<K, R>> build(@Nonnull AggregateOperation<?, ? extends R> aggrOp) {
        return grAggBuilder.buildStream(aggrOp);
    }
}
