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

package com.hazelcast.jet.stream;

import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.stream.impl.collectors.DistributedCollectorImpl;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * <i>A mutable reduction operation</i> that
 * accumulates input elements into a mutable result container, optionally transforming
 * the accumulated result into a final representation after all input elements
 * have been processed.
 *
 * @param <T> the type of input elements to the reduction operation
 * @param <A> the mutable accumulation type of the reduction operation (often
 *            hidden as an implementation detail)
 * @param <R> the result type of the reduction operation
 * @see java.util.stream.Collector
 * @see DistributedCollectors
 */
public interface DistributedCollector<T, A, R> extends java.util.stream.Collector<T, A, R>, Serializable {

    /**
     * A function that creates and returns a new mutable result container.
     *
     * @return a function which returns a new, mutable result container
     */
    @Override
    Supplier<A> supplier();

    /**
     * A function that folds a value into a mutable result container.
     *
     * @return a function which folds a value into a mutable result container
     */
    @Override
    BiConsumer<A, T> accumulator();

    /**
     * A function that accepts two partial results and merges them.  The
     * combiner function may fold state from one argument into the other and
     * return that, or may return a new result container.
     *
     * @return a function which combines two partial results into a combined
     * result
     */
    @Override
    BinaryOperator<A> combiner();

    /**
     * Perform the final transformation from the intermediate accumulation type
     * {@code A} to the final result type {@code R}.
     *
     * <p>If the characteristic {@code IDENTITY_TRANSFORM} is
     * set, this function may be presumed to be an identity transform with an
     * unchecked cast from {@code A} to {@code R}.
     *
     * @return a function which transforms the intermediate result to the final
     * result
     */
    @Override
    Function<A, R> finisher();

    /**
     * Returns a new {@code Distributed.Collector} described by the given {@code supplier},
     * {@code accumulator}, and {@code combiner} functions.  The resulting
     * {@code Distributed.Collector} has the {@code Collector.Characteristics.IDENTITY_FINISH}
     * characteristic.
     *
     * @param supplier        The supplier function for the new collector
     * @param accumulator     The accumulator function for the new collector
     * @param combiner        The combiner function for the new collector
     * @param characteristics The collector characteristics for the new
     *                        collector
     * @param <T>             The type of input elements for the new collector
     * @param <R>             The type of intermediate accumulation result, and final result,
     *                        for the new collector
     * @return the new {@code Distributed.Collector}
     * @throws NullPointerException if any argument is null
     */
    static <T, R> DistributedCollector<T, R, R> of(Supplier<R> supplier,
                                                   BiConsumer<R, T> accumulator,
                                                   BinaryOperator<R> combiner,
                                                   Characteristics... characteristics) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        Objects.requireNonNull(characteristics);
        Set<Characteristics> cs = (characteristics.length == 0)
                ? DistributedCollectors.CH_ID
                : Collections.unmodifiableSet(EnumSet.of(Characteristics.IDENTITY_FINISH,
                characteristics));
        return new DistributedCollectorImpl<>(supplier, accumulator, combiner, cs);
    }

    /**
     * Returns a new {@code Distributed.Collector} described by the given {@code supplier},
     * {@code accumulator}, {@code combiner}, and {@code finisher} functions.
     *
     * @param supplier The supplier function for the new collector
     * @param accumulator The accumulator function for the new collector
     * @param combiner The combiner function for the new collector
     * @param finisher The finisher function for the new collector
     * @param characteristics The collector characteristics for the new
     *                        collector
     * @param <T> The type of input elements for the new collector
     * @param <A> The intermediate accumulation type of the new collector
     * @param <R> The final result type of the new collector
     * @throws NullPointerException if any argument is null
     * @return the new {@code Distributed.Collector}
     */
    static <T, A, R> DistributedCollector<T, A, R> of(Supplier<A> supplier,
                                                      BiConsumer<A, T> accumulator,
                                                      BinaryOperator<A> combiner,
                                                      Function<A, R> finisher,
                                                      Characteristics... characteristics) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(combiner);
        Objects.requireNonNull(finisher);
        Objects.requireNonNull(characteristics);
        Set<Characteristics> cs = DistributedCollectors.CH_NOID;
        if (characteristics.length > 0) {
            cs = EnumSet.noneOf(Characteristics.class);
            Collections.addAll(cs, characteristics);
            cs = Collections.unmodifiableSet(cs);
        }
        return new DistributedCollectorImpl<>(supplier, accumulator, combiner, finisher, cs);
    }

    /**
     * Executes the collector with the given context and upstream pipeline.
     *
     * @param context the context of the stream
     * @param upstream the upstream pipeline to execute the stream on
     * @return the result of the executed collector
     */
    R collect(StreamContext context, Pipeline<? extends T> upstream);
}
