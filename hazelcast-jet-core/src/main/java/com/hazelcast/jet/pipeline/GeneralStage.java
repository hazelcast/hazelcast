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
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.function.DistributedTriFunction;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;

/**
 * Represents the common aspect of {@link BatchStage batch} and {@link
 * StreamStage stream} pipeline stages, defining those operations that
 * apply to both.
 * <p>
 * Unless specified otherwise, all functions passed to methods of this
 * interface must be stateless.
 *
 * @param <T> the type of items coming out of this stage
 */
public interface GeneralStage<T> extends Stage {

    /**
     * Attaches to this stage a mapping stage, one which applies the supplied
     * function to each input item independently and emits the function's
     * result as the output item. If the result is {@code null}, it emits
     * nothing. Therefore this stage can be used to implement filtering
     * semantics as well.
     *
     * @param mapFn a stateless mapping function
     * @param <R> the result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    /**
     * Attaches to this stage a filtering stage, one which applies the provided
     * predicate function to each input item to decide whether to pass the item
     * to the output or to discard it. Returns the newly attached stage.
     *
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     */
    @Nonnull
    GeneralStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    /**
     * Attaches to this stage a flat-mapping stage, one which applies the
     * supplied function to each input item independently and emits all the
     * items from the {@link Traverser} it returns. The traverser must be
     * <em>null-terminated</em>.
     *
     * @param flatMapFn a stateless flatmapping function, whose result type is
     *                  Jet's {@link Traverser}
     * @param <R> the type of items in the result's traversers
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> flatMap(
            @Nonnull DistributedFunction<? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Attaches to both this and the supplied stage a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet
     * package Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1        the stage to hash-join with this one
     * @param joinClause1   specifies how to join the two streams
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K>           the type of the join key
     * @param <T1_IN>       the type of {@code stage1} items
     * @param <T1>          the result type of projection on {@code stage1} items
     * @param <R>           the resulting output type
     * @return the newly attached stage
     */
    @Nonnull
    <K, T1_IN, T1, R> GeneralStage<R> hashJoin(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull DistributedBiFunction<T, T1, R> mapToOutputFn
    );

    /**
     * Attaches to this and the two supplied stages a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1        the first stage to join
     * @param joinClause1   specifies how to join with {@code stage1}
     * @param stage2        the second stage to join
     * @param joinClause2   specifices how to join with {@code stage2}
     * @param mapToOutputFn function to map the joined items to the output value
     * @param <K1>          the type of key for {@code stage1}
     * @param <T1_IN>       the type of {@code stage1} items
     * @param <T1>          the result type of projection of {@code stage1} items
     * @param <K2>          the type of key for {@code stage2}
     * @param <T2_IN>       the type of {@code stage2} items
     * @param <T2>          the result type of projection of {@code stage2} items
     * @param <R>           the resulting output type
     * @return the newly attached stage
     */
    @Nonnull
    <K1, T1_IN, T1, K2, T2_IN, T2, R> GeneralStage<R> hashJoin2(
            @Nonnull BatchStage<T1_IN> stage1,
            @Nonnull JoinClause<K1, ? super T, ? super T1_IN, ? extends T1> joinClause1,
            @Nonnull BatchStage<T2_IN> stage2,
            @Nonnull JoinClause<K2, ? super T, ? super T2_IN, ? extends T2> joinClause2,
            @Nonnull DistributedTriFunction<T, T1, T2, R> mapToOutputFn
    );

    /**
     * Returns a fluent API builder object to construct a hash join operation
     * with any number of contributing stages. It is mainly intended for
     * hash-joins with three or more enriching stages. For one or two stages
     * prefer the direct {@code stage.hashJoinN(...)} calls because they offer
     * more static type safety.
     */
    @Nonnull
    GeneralHashJoinBuilder<T> hashJoinBuilder();

    /**
     * Specifes the function that will extract the grouping key from the items
     * in the associated pipeline stage, as first step in the construction of a
     * group-and-aggregate stage.
     *
     * @param keyFn function that extracts the grouping key
     * @param <K> type of the key
     */
    @Nonnull
    <K> GeneralStageWithGrouping<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    /**
     * Adds a timestamp to each item in the stream using the current system
     * time.
     *
     * @throws IllegalArgumentException if timestamps were already added to the stream
     */
    @Nonnull
    default StreamStage<T> addTimestamps() {
        return addTimestamps(t -> System.currentTimeMillis(), 0);
    }

    /**
     * Adds a timestamp to each item in the stream using the supplied function
     * and specifies the allowed amount of disorder between them. As the stream
     * moves on the timestamps must increase, but you can tell Jet to accept
     * some items that "come in late", i.e., have a lower timestamp than the
     * items before them. The {@code allowedLag} parameter controls how much
     * lower the timestamp can be than the highest one observed so far. If
     * it is even lower, Jet will drop the item as being "too late".
     * <p>
     * For example, if the sequence of the timestamps is {@code [1,4,3,2]} and
     * you configured the allowed lag as {@code 1}, Jet will let through the
     * event with timestamp {@code 3}, but it will drop the last one (timestamp
     * {@code 2}).
     * <p>
     * The amount of lag you configure strongly influences the latency of Jet's
     * output. Jet cannot finalize the window until it knows it has observed all
     * the events belonging to it, and the more lag it must tolerate, the longer
     * will it have to wait for possible latecomers. On the other hand, if don't
     * allow enough lag, you face the risk of failing to account for the data
     * that came in after the results were already emitted.
     *
     * @param timestampFn a function that returns the timestamp for each item
     * @param allowedLag the allowed lag behind the top observed timestamp
     *
     * @throws IllegalArgumentException if timestamps were already added to the stream
     */
    @Nonnull
    StreamStage<T> addTimestamps(DistributedToLongFunction<? super T> timestampFn, long allowedLag);

    /**
     * Attaches to this stage a sink stage, one that accepts data but doesn't
     * emit any. The supplied argument specifies what to do with the received
     * data (typically push it to some outside resource).
     *
     * @return the newly attached sink stage
     */
    @Nonnull
    SinkStage drainTo(@Nonnull Sink<? super T> sink);

    /**
     * Attaches a peeking stage which logs this stage's output and passes it
     * through without transformation. For each item the stage emits, it:
     * <ol><li>
     *     uses the {@code shouldLogFn} predicate to see whether to log the item
     * </li><li>
     *     if yes, uses then uses {@code toStringFn} to get the item's string
     *     representation
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @param shouldLogFn a function to filter the logged items. You can use {@link
     *                    com.hazelcast.jet.function.DistributedFunctions#alwaysTrue()
     *                    alwaysTrue()} as a pass-through filter when you don't need any
     *                    filtering.
     * @param toStringFn  a function that returns a string representation of the item
     * @see #peek(DistributedFunction)
     * @see #peek()
     */
    @Nonnull
    GeneralStage<T> peek(
            @Nonnull DistributedPredicate<? super T> shouldLogFn,
            @Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn
    );

    /**
     * Adds a peeking layer to this compute stage which logs its output. For
     * each item the stage emits, it:
     * <ol><li>
     *     uses {@code toStringFn} to get a string representation of the item
     * </li><li>
     *     logs the string at the INFO level to the log category {@code
     *     com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}
     * </li></ol>
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @param toStringFn  a function that returns a string representation of the item
     * @see #peek(DistributedPredicate, DistributedFunction)
     * @see #peek()
     */
    @Nonnull
    default GeneralStage<T> peek(@Nonnull DistributedFunction<? super T, ? extends CharSequence> toStringFn) {
        return peek(alwaysTrue(), toStringFn);
    }

    /**
     * Adds a peeking layer to this compute stage which logs its output. For
     * each item the stage emits, it logs the result of its {@code toString()}
     * method at the INFO level to the log category {@code
     * com.hazelcast.jet.impl.processor.PeekWrappedP.<vertexName>#<processorIndex>}.
     * The stage logs each item on whichever cluster member it happens to
     * receive it. Its primary purpose is for development use, when running Jet
     * on a local machine.
     *
     * @see #peek(DistributedPredicate, DistributedFunction)
     * @see #peek(DistributedFunction)
     */
    @Nonnull
    default GeneralStage<T> peek() {
        return peek(alwaysTrue(), Object::toString);
    }

    /**
     * Attaches to this stage a stage with a custom transform based on the
     * provided supplier of Core API {@link Processor}s. To be compatible with
     * the rest of the stage, the processor must expect a single inbound
     * edge and arbitrarily many outbound edges, and it must push the same data
     * to all outbound edges.
     * <p>
     * Note that the returned stage's type parameter is inferred from the call
     * site and not propagated from the processor that will produce the result,
     * so there is no actual type safety provided.
     *
     * @param stageName a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     * @param <R> the type of the output items
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(
            @Nonnull String stageName, @Nonnull DistributedSupplier<Processor> procSupplier);
}
