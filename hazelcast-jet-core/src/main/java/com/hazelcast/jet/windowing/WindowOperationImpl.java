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
import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;

import javax.annotation.Nonnull;

class WindowOperationImpl<T, A, R> implements WindowOperation<T, A, R> {
    private final Distributed.Supplier<A> createAccumulatorF;
    private final Distributed.BiConsumer<A, T> accumulateItemF;
    private final Distributed.BinaryOperator<A> combineAccumulatorsF;
    private final Distributed.BinaryOperator<A> deductAccumulatorF;
    private final Distributed.Function<A, R> finishAccumulationF;

    WindowOperationImpl(Supplier<A> createAccumulatorF,
                        BiConsumer<A, T> accumulateItemF,
                        BinaryOperator<A> combineAccumulatorsF,
                        BinaryOperator<A> deductAccumulatorF,
                        Function<A, R> finishAccumulationF
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
    public Distributed.BiConsumer<A, T> accumulateItemF() {
        return accumulateItemF;
    }

    @Override @Nonnull
    public Distributed.BinaryOperator<A> combineAccumulatorsF() {
        return combineAccumulatorsF;
    }

    @Override @Nonnull
    public BinaryOperator<A> deductAccumulatorF() {
        return deductAccumulatorF;
    }

    @Override @Nonnull
    public Distributed.Function<A, R> finishAccumulationF() {
        return finishAccumulationF;
    }
}
