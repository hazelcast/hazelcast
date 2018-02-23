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

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.TimestampedItem;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.WindowResultFunction;

import javax.annotation.Nonnull;

/**
 * Javadoc pending.
 */
public interface StageWithWindow<T> {

    @Nonnull
    WindowDefinition windowDefinition();

    @Nonnull
    StreamStage<T> streamStage();

    @Nonnull
    <K> StageWithGroupingAndWindow<T, K> groupingKey(
            @Nonnull DistributedFunction<? super T, ? extends K> keyFn
    );

    @Nonnull
    <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn
    );

    @Nonnull
    default <A, R> StreamStage<TimestampedItem<R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggregate(aggrOp, TimestampedItem::new);
    }

    @Nonnull
    <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    @Nonnull
    default <T1, A, R> StreamStage<TimestampedItem<R>> aggregate2(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        return aggregate2(stage1, aggrOp, TimestampedItem::new);
    }

    @Nonnull
    <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull WindowResultFunction<? super R, ? extends OUT> mapToOutputFn);

    @Nonnull
    default <T1, T2, A, R> StreamStage<TimestampedItem<R>> aggregate3(
            @Nonnull StreamStage<T1> stage1,
            @Nonnull StreamStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggregate3(stage1, stage2, aggrOp, TimestampedItem::new);
    }

    @Nonnull
    default WindowAggregateBuilder<T> aggregateBuilder() {
        return new WindowAggregateBuilder<>(streamStage(), windowDefinition());
    }
}
