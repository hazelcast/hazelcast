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

import com.hazelcast.core.IMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
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
     * Attaches a mapping stage which applies the supplied function to each
     * input item independently and emits the function's result as the output
     * item. If the result is {@code null}, it emits nothing. Therefore this
     * stage can be used to implement filtering semantics as well.
     *
     * @param mapFn a stateless mapping function
     * @param <R> the result type of the mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> map(@Nonnull DistributedFunction<? super T, ? extends R> mapFn);

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. Returns the newly attached stage.
     *
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     */
    @Nonnull
    GeneralStage<T> filter(@Nonnull DistributedPredicate<T> filterFn);

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all the items from the {@link
     * Traverser} it returns. The traverser must be <em>null-terminated</em>.
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
     * Attaches a mapping stage which applies the supplied function to each
     * input item independently and emits the function's result as the output
     * item. The mapping function receives another parameter, the context
     * object, which Jet will create using the supplied {@code contextFactory}.
     * <p>
     * If the mapping result is {@code null}, it emits nothing. Therefore this
     * stage can be used to implement filtering semantics as well.
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the context object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <C> type of context object
     * @param <R> the result type of the mapping function
     * @param contextFactory the context factory
     * @param mapFn a stateless mapping function
     * @return the newly attached stage
     */
    @Nonnull
    <C, R> GeneralStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends R> mapFn
    );

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. The predicate function receives another parameter, the
     * context object, which Jet will create using the supplied {@code
     * contextFactory}.
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the context object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <C> type of context object
     * @param contextFactory the context factory
     * @param filterFn a stateless filter predicate function
     * @return the newly attached stage
     */
    @Nonnull
    <C> GeneralStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiPredicate<? super C, ? super T> filterFn
    );

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all items from the {@link
     * Traverser} it returns as the output items. The traverser must be
     * <em>null-terminated</em>. The mapping function receives another
     * parameter, the context object, which Jet will create using the supplied
     * {@code contextFactory}.
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the context object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <C> type of context object
     * @param <R> the type of items in the result's traversers
     * @param contextFactory the context factory
     * @param flatMapFn a stateless flatmapping function, whose result type is
     *                  Jet's {@link Traverser}
     * @return the newly attached stage
     */
    @Nonnull
    <C, R> GeneralStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code ReplicatedMap} with the supplied name. The mapping
     * function will receive it as the first argument.
     *
     * @param mapName name of the {@code ReplicatedMap}
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code ReplicatedMap}
     * @param <V> type of the value in the {@code ReplicatedMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingReplicatedMap(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<? super ReplicatedMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return mapUsingContext(ContextFactories.replicatedMapContext(mapName), mapFn);
    }

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code ReplicatedMap}. <strong>It is not necessarily the
     * map you provide here</strong>, but a replicated map with the same name
     * in the Jet cluster that executes the pipeline. The mapping function
     * will receive the replicated map as the first argument.
     *
     * @param replicatedMap the {@code ReplicatedMap} to use as context
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code ReplicatedMap}
     * @param <V> type of the value in the {@code ReplicatedMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingReplicatedMap(
            @Nonnull ReplicatedMap<K, V> replicatedMap,
            @Nonnull DistributedBiFunction<? super ReplicatedMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return mapUsingReplicatedMap(replicatedMap.getName(), mapFn);
    }

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code IMap} with the supplied name. The mapping function
     * will receive it as the first argument.
     *
     * @param mapName name of the {@code IMap}
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code IMap}
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull DistributedBiFunction<? super IMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return mapUsingContext(ContextFactories.iMapContext(mapName), mapFn);
    }

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code IMap}. <strong>It is not necessarily the map you
     * provide here</strong>, but a map with the same name in the Jet cluster
     * that executes the pipeline. The mapping function will receive the
     * replicated map as the first argument.
     *
     * @param iMap the {@code IMap} to use as the context
     * @param mapFn the mapping function
     * @param <K> type of the key in the {@code IMap}
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <K, V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull DistributedBiFunction<? super IMap<K, V>, ? super T, ? extends R> mapFn
    ) {
        return mapUsingIMap(iMap.getName(), mapFn);
    }

    /**
     * Attaches a rolling aggregation stage. As opposed to regular aggregation,
     * this stage emits the current aggregation result after receiving each
     * item. For example, if your aggregation is <em>summing</em> and the input
     * is {@code {2, 7, 8, -5}}, the output will be {@code {2, 9, 17, 12}}. The
     * number of input and output items is equal.
     * <p>
     * This stage is fault-tolerant and saves its state to the snapshot.
     * <p>
     * <strong>NOTE 1:</strong> since the output for each item depends on all
     * the previous items, this operation cannot be parallelized. Jet will
     * perform it on a single member, single-threaded.
     * <p>
     * <strong>NOTE 2:</strong> if you plan to use an aggregate operation whose
     * result size grows with input size (such as {@code toList} and your data
     * source is unbounded, you must carefully consider the memory demands this
     * implies. The result will keep growing forever.
     *
     * @param aggrOp the aggregate operation to do the aggregation
     * @param <R> result type of the aggregate operation
     *
     * @return the newly attached stage
     */
    @Nonnull
    <R> GeneralStage<R> rollingAggregate(@Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp);

    /**
     * Attaches to both this and the supplied stage a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package javadoc} for a detailed description of the hash-join transform.
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
     * package javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1        the first stage to join
     * @param joinClause1   specifies how to join with {@code stage1}
     * @param stage2        the second stage to join
     * @param joinClause2   specifies how to join with {@code stage2}
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
    <K1, K2, T1_IN, T2_IN, T1, T2, R> GeneralStage<R> hashJoin2(
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
     * Specifies the function that will extract a key from the items in the
     * associated pipeline stage. This enables the operations that need the
     * key, such as grouped aggregation. The key must not be null and must
     * properly implement {@code equals()} and {@code hashCode()}.
     *
     * @param keyFn function that extracts the grouping key
     * @param <K> type of the key
     */
    @Nonnull
    <K> GeneralStageWithKey<T, K> groupingKey(@Nonnull DistributedFunction<? super T, ? extends K> keyFn);

    /**
     * Adds a timestamp to each item in the stream using the current system
     * time. <strong>NOTE:</strong> when snapshotting is enabled to achieve
     * fault tolerance, after a restart Jet replays all the events that were
     * already processed since the last snapshot. These events will now get
     * different timestamps. If you want your job to be fault-tolerant, the
     * events in the stream must carry their own timestamp and you must use
     * {@link #addTimestamps(DistributedToLongFunction, long)
     * addTimestamps(timestampFn, allowedLag} to extract them.
     *
     * @throws IllegalArgumentException if this stage already has timestamps
     */
    @Nonnull
    default StreamStage<T> addTimestamps() {
        return addTimestamps(t -> System.currentTimeMillis(), 0);
    }

    /**
     * Adds a timestamp to each item in the stream using the supplied function
     * and specifies the allowed amount of disorder between them. As the stream
     * moves on, the timestamps must increase, but you can tell Jet to accept
     * some items that "come in late", i.e., have a lower timestamp than the
     * items before them. The {@code allowedLag} parameter controls by how much
     * the timestamp can be lower than the highest one observed so far. If
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
     * will it have to wait for possible latecomers. On the other hand, if you
     * don't allow enough lag, you face the risk of failing to account for the
     * data that came in after the results were already emitted.
     * <p>
     * You should strongly prefer adding this stage right after the source. In
     * that case Jet can compute the watermark inside the source connector,
     * taking into account its partitioning. It can maintain a separate
     * watermark value for each partition and coalesce them into the overall
     * watermark without causing dropped events. If you add the timestamps
     * later on, events from different partitions may be mixed, increasing
     * the perceived event lag and causing more dropped events.
     * <p>
     * <b>Warning:</b> make sure the property you access in {@code timestampFn}
     * isn't null, it would fail the job. Also that there are no nonsensical
     * values such as -1, MIN_VALUE, 2100-01-01 etc - we'll treat those as real
     * timestamps and they can cause unspecified behaviour.
     *
     * @param timestampFn a function that returns the timestamp for each item,
     *                    typically in milliseconds
     * @param allowedLag the allowed lag behind the top observed timestamp.
     *                   Time unit is the same as the unit used by {@code
     *                   timestampFn}
     *
     * @throws IllegalArgumentException if this stage already has timestamps
     */
    @Nonnull
    StreamStage<T> addTimestamps(@Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLag);

    /**
     * Attaches a sink stage, one that accepts data but doesn't
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
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s.
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
