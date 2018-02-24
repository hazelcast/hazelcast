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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.stream.impl.pipeline.Pipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.reducers.DistributedCollectorImpl;

import java.io.Serializable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * {@code Serializable} variant of {@link Collector java.util.stream.Collector}.
 */
public interface DistributedCollector<T, A, R> extends Collector<T, A, R>, Serializable {

    /**
     * {@code Serializable} variant of {@link
     * Collector#of(Supplier, BiConsumer, BinaryOperator, Characteristics...)
     * java.util.stream.Collector#of(Supplier, BiConsumer, BinaryOperator, Characteristics...) }
     */
    static <T, R> DistributedCollector<T, R, R> of(DistributedSupplier<R> supplier,
                                                   DistributedBiConsumer<R, T> accumulator,
                                                   DistributedBinaryOperator<R> combiner) {
        return of(supplier, accumulator, combiner, identity());
    }

    /**
     * {@code Serializable} variant of {@link
     * Collector#of(Supplier, BiConsumer, BinaryOperator, Function, Characteristics...)
     * java.util.stream.Collector#of(Supplier, BiConsumer, BinaryOperator, Function, Characteristics...) }
     */
    static <T, A, R> DistributedCollector<T, A, R> of(DistributedSupplier<A> supplier,
                                                      DistributedBiConsumer<A, T> accumulator,
                                                      DistributedBinaryOperator<A> combiner,
                                                      DistributedFunction<A, R> finisher) {
        checkNotNull(supplier, "supplier");
        checkNotNull(accumulator, "accumulator");
        checkNotNull(combiner, "combiner");
        checkNotNull(finisher, "finisher");
        return new DistributedCollectorImpl<>(supplier, accumulator, combiner, finisher);
    }

    @Override
    DistributedSupplier<A> supplier();

    @Override
    DistributedBiConsumer<A, T> accumulator();

    @Override
    DistributedBinaryOperator<A> combiner();

    @Override
    DistributedFunction<A, R> finisher();

    @Override
    default Set<Characteristics> characteristics() {
        throw new UnsupportedOperationException("characteristics is not supported for DistributedCollector");
    }

    /**
     * Interface for Jet-specific distributed reducers which execute the
     * terminal reduce operation over the current {@code DistributedStream}
     * by building and executing a DAG. These reducers can't be used as
     * downstream collectors.
     *
     * @param <T> the type of input elements to the reduction operation
     * @param <R> the result type of the reduction operation
     */
    interface Reducer<T, R> extends Serializable {

        /**
         * Executes the reducer with the given context and upstream pipeline.
         *
         * @param context  the context of the stream
         * @param upstream the upstream pipeline to execute the stream on
         * @return the result of the executed collector
         */
        R reduce(StreamContext context, Pipe<? extends T> upstream);
    }
}
