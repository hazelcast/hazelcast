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

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Represents a stage in a distributed computation {@link Pipeline
 * pipeline} that will observe a finite amount of data (a batch). It
 * accepts input from its upstream stages (if any) and passes its output
 * to the downstream stages.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface BatchStage<T> extends GeneralStage<T> {

    @Nonnull
    <K> StageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    @Nonnull @Override
    <R> BatchStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    @Nonnull @Override
    BatchStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    @Nonnull @Override
    <R> BatchStage<R> flatMap(@Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn);

    @Nonnull @Override
    <K, T1_IN, T1, R> BatchStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    );

    @Nonnull @Override
    <K1, T1_IN, T1, K2, T2_IN, T2, R> BatchStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    );

    @Nonnull @Override
    default HashJoinBuilder<T> hashJoinBuilder() {
        return new HashJoinBuilder<>(this);
    }

    @Nonnull @Override
    default BatchStage<T> peek() {
        return (BatchStage<T>) GeneralStage.super.peek();
    }

    @Nonnull @Override
    BatchStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    );

    @Nonnull @Override
    default BatchStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return (BatchStage<T>) GeneralStage.super.peek(toStringFn);
    }

    @Nonnull @Override
    <R> BatchStage<R> customTransform(
            @Nonnull String stageName, @Nonnull DistributedSupplier<Processor> procSupplier);

    @Nonnull
    <A, R> BatchStage<R> aggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    );

    @Nonnull
    <T1, A, R> BatchStage<R> aggregate2(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull AggregateOperation2<? super T, ? super T1, A, ? extends R> aggrOp);

    @Nonnull
    <T1, T2, A, R> BatchStage<R> aggregate3(
            @Nonnull BatchStage<T1> stage1,
            @Nonnull BatchStage<T2> stage2,
            @Nonnull AggregateOperation3<? super T, ? super T1, ? super T2, A, ? extends R> aggrOp);

    @Nonnull
    default AggregateBuilder<T> aggregateBuilder() {
        return new AggregateBuilder<>(this);
    }

    @Nonnull @Override
    BatchStage<T> setLocalParallelism(int localParallelism);

    @Nonnull @Override
    BatchStage<T> setName(@Nullable String name);
}
