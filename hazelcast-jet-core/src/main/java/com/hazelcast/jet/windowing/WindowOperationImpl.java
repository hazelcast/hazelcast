/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.windowing;

import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.BinaryOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

class WindowOperationImpl<T, A, R> implements WindowOperation<T, A, R> {
    private final Distributed.Supplier<A> createAccumulatorF;
    private final Distributed.BiFunction<A, T, A> accumulateItemF;
    private final Distributed.BinaryOperator<A> combineAccumulatorsF;
    private final Distributed.BinaryOperator<A> deductAccumulatorF;
    private final Distributed.Function<A, R> finishAccumulationF;

    WindowOperationImpl(Distributed.Supplier<A> createAccumulatorF,
                        Distributed.BiFunction<A, T, A> accumulateItemF,
                        Distributed.BinaryOperator<A> combineAccumulatorsF,
                        Distributed.BinaryOperator<A> deductAccumulatorF,
                        Distributed.Function<A, R> finishAccumulationF
    ) {
        this.createAccumulatorF = createAccumulatorF;
        this.accumulateItemF = accumulateItemF;
        this.combineAccumulatorsF = combineAccumulatorsF;
        this.deductAccumulatorF = deductAccumulatorF;
        this.finishAccumulationF = finishAccumulationF;
    }

    @Override @Nonnull
    public Distributed.Supplier<A> createAccumulatorF() {
        return createAccumulatorF;
    }

    @Override @Nonnull
    public Distributed.BiFunction<A, T, A> accumulateItemF() {
        return accumulateItemF;
    }

    @Override @Nonnull
    public Distributed.BinaryOperator<A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Override @Nullable
    public BinaryOperator<A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Override @Nonnull
    public Distributed.Function<A, R> finishAccumulationF() {
        return finishAccumulationF;
    }
}
