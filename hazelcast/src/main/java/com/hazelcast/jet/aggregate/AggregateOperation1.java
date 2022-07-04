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

package com.hazelcast.jet.aggregate;

import com.hazelcast.function.BiConsumerEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.jet.core.Processor;

import javax.annotation.Nonnull;
import java.util.stream.Collector;

/**
 * Specialization of {@code AggregateOperation} (refer to its {@linkplain
 * AggregateOperation extensive documentation}) to the "arity-1" case with
 * a single data stream being aggregated over. {@link AggregateOperations}
 * contains factories for the built-in implementations and you can create
 * your own using the {@linkplain AggregateOperation#withCreate aggregate
 * operation builder}.
 * <p>
 * All the functions must be stateless and {@linkplain
 * Processor#isCooperative() cooperative}.
 *
 * @param <T> the type of the stream item
 * @param <A> the type of the accumulator
 * @param <R> the type of the aggregation result
 *
 * @since Jet 3.0
 */
public interface AggregateOperation1<T, A, R> extends AggregateOperation<A, R> {

    /**
     * A primitive that updates the accumulator state to account for a new
     * item.
     * <p>
     * The consumer must be stateless and {@linkplain Processor#isCooperative()
     * cooperative}.
     */
    @Nonnull
    BiConsumerEx<? super A, ? super T> accumulateFn();

    /**
     * Returns a copy of this aggregate operation, but with the {@code
     * accumulate} primitive replaced with the one supplied here.
     * <p>
     * The consumer must be stateless and {@linkplain Processor#isCooperative()
     * cooperative}.
     */
    @Nonnull
    <NEW_T> AggregateOperation1<NEW_T, A, R> withAccumulateFn(
            BiConsumerEx<? super A, ? super NEW_T> accumulateFn
    );

    // Narrows the return type
    @Nonnull @Override
    AggregateOperation1<T, A, A> withIdentityFinish();

    // Narrows the return type
    @Nonnull @Override
    <R_NEW> AggregateOperation1<T, A, R_NEW> andThen(FunctionEx<? super R, ? extends R_NEW> thenFn);

    /**
     * @deprecated see {@linkplain AggregateOperations#toCollector(AggregateOperation1)}
     */
    @Nonnull
    @Deprecated
    default Collector<T, A, R> toCollector() {
        return AggregateOperations.toCollector(this);
    }
}
