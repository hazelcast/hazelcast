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
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.KeyedWindowResultFunction;

import javax.annotation.Nonnull;

/**
 * Javadoc pending.
 */
public interface StageWithGroupingAndWindow<T, K> {

    @Nonnull
    DistributedFunction<? super T, ? extends K> keyFn();

    @Nonnull
    WindowDefinition windowDefinition();

    @Nonnull
    @SuppressWarnings("unchecked")
    <A, R, OUT> StreamStage<OUT> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    );

    @Nonnull
    <A, R> StreamStage<TimestampedEntry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp);

    @Nonnull
    <T1, A, R, OUT> StreamStage<OUT> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn
    );

    @Nonnull
    <T1, A, R> StreamStage<TimestampedEntry<K, R>> aggregate2(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp);

    @Nonnull
    <T1, T2, A, R, OUT> StreamStage<OUT> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull KeyedWindowResultFunction<? super K, ? super R, OUT> mapToOutputFn);

    @Nonnull
    <T1, T2, A, R> StreamStage<TimestampedEntry<K, R>> aggregate3(
            @Nonnull StreamStageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StreamStageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp);

    @Nonnull
    WindowGroupAggregateBuilder<T, K> aggregateBuilder();
}
