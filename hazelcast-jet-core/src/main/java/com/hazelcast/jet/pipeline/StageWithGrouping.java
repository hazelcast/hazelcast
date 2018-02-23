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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedBiFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Javadoc pending.
 */
public interface StageWithGrouping<T, K> extends GeneralStageWithGrouping<T, K> {

    @Nonnull
    <A, R, OUT> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn);

    @Nonnull
    default <A, R> BatchStage<Entry<K, R>> aggregate(
            @Nonnull AggregateOperation1<? super T, A, R> aggrOp
    ) {
        return aggregate(aggrOp, Util::entry);
    }

    @Nonnull
    <T1, A, R, OUT> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn);

    @Nonnull
    default <T1, A, R> BatchStage<Entry<K, R>> aggregate2(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, R> aggrOp
    ) {
        return aggregate2(stage1, aggrOp, Util::entry);
    }

    @Nonnull
    <T1, T2, A, R, OUT> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, OUT> mapToOutputFn);

    @Nonnull
    default <T1, T2, A, R> BatchStage<Entry<K, R>> aggregate3(
            @Nonnull StageWithGrouping<T1, ? extends K> stage1,
            @Nonnull StageWithGrouping<T2, ? extends K> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, R> aggrOp
    ) {
        return aggregate3(stage1, stage2, aggrOp, Util::entry);
    }

    @Nonnull
    default GroupAggregateBuilder<T, K> aggregateBuilder() {
        return new GroupAggregateBuilder<>(this);
    }
}
