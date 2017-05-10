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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.stream.DistributedCollector;

import java.util.Set;

public class DistributedCollectorImpl<T, A, R> implements DistributedCollector<T, A, R> {

    private final DistributedSupplier<A> supplier;
    private final DistributedBiConsumer<A, T> accumulator;
    private final DistributedBinaryOperator<A> combiner;
    private final Set<Characteristics> characteristics;
    private final DistributedFunction<A, R> finisher;

    public DistributedCollectorImpl(
            DistributedSupplier<A> supplier, DistributedBiConsumer<A, T> accumulator,
            DistributedBinaryOperator<A> combiner, DistributedFunction<A, R> finisher,
            Set<Characteristics> characteristics
    ) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
        this.finisher = finisher;
        this.characteristics = characteristics;
    }

    public DistributedCollectorImpl(
            DistributedSupplier<A> supplier, DistributedBiConsumer<A, T> accumulator,
            DistributedBinaryOperator<A> combiner, Set<Characteristics> characteristics
    ) {
        this(supplier, accumulator, combiner, castingIdentity(), characteristics);
    }

    static <I, R> DistributedFunction<I, R> castingIdentity() {
        return i -> (R) i;
    }

    @Override
    public DistributedSupplier<A> supplier() {
        return supplier;
    }

    @Override
    public DistributedBiConsumer<A, T> accumulator() {
        return accumulator;
    }

    @Override
    public DistributedBinaryOperator<A> combiner() {
        return combiner;
    }

    @Override
    public DistributedFunction<A, R> finisher() {
        return finisher;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return characteristics;
    }
}
