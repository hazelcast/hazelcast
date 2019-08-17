/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.BiPredicateEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * An intermediate step while constructing a pieline transform that
 * involves a grouping key, such as windowed group-and-aggregate. Some
 * transforms use the grouping key only to partition the stream (such as
 * {@link #mapUsingIMap}).
 *
 * @param <T> type of the stream items
 * @param <K> type of the key
 *
 * @since 3.0
 */
public interface StreamStageWithKey<T, K> extends GeneralStageWithKey<T, K> {

    /**
     * Adds the definition of the window to use in the group-and-aggregate
     * pipeline stage being constructed.
     */
    @Nonnull
    StageWithKeyAndWindow<T, K> window(@Nonnull WindowDefinition wDef);

    /**
     * Attaches a stage that performs a stateful mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code mapFn}, which can update
     * the object's state. For each grouping key there's a separate state
     * object. The state object will be included in the state snapshot, so it
     * survives job restarts. For this reason the object must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object
     * has a timestamp attached to it: the top timestamp of any event with the
     * same key seen so far. Upon seeing another event, Jet compares the state
     * timestamp with the current watermark. If it is less than {@code wm - ttl},
     * it discards the state object and creates a new one before processing the
     * event.
     * <p>
     * Sample usage:
     * <pre>{<code
     *
     * }</pre>
     *
     * @param createFn the function that returns the state object
     * @param mapFn    the function that receives the state object and the input item and
     *                 outputs the result item. It may modify the state object.
     * @param <S>      type of the state object
     * @param <R>      type of the result
     */
    @Nonnull
    <S, R> StreamStage<Entry<K, R>> mapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    default <S, R> StreamStage<Entry<K, R>> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends R> mapFn
    ) {
        return mapStateful(0, createFn, mapFn);
    }

    /**
     * Attaches a stage that performs a stateful filtering operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code filterFn}, which can update
     * the object's state. For each grouping key there's a separate state
     * object. The state object will be included in the state snapshot, so it
     * survives job restarts. For this reason the object must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object
     * has a timestamp attached to it: the top timestamp of any event with the
     * same key seen so far. Upon seeing another event, Jet compares the state
     * timestamp with the current watermark. If it is less than {@code wm - ttl},
     * it discards the state object and creates a new one before processing the
     * event.
     * <p>
     * Sample usage:
     * <pre>{<code
     *
     * }</pre>
     *
     * @param createFn the function that returns the state object
     * @param filterFn predicate that receives the state object and the input item and
     *                 outputs a boolean value. It may modify the state object.
     * @param <S>      type of the state object
     */
    @Nonnull
    <S> StreamStage<Entry<K, T>> filterStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    @Nonnull @Override
    default <S> StreamStage<Entry<K, T>> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    ) {
        return filterStateful(0, createFn, filterFn);
    }

    /**
     * Attaches a stage that performs a stateful flat-mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code flatMapFn}, which can update
     * the object's state. For each grouping key there's a separate state
     * object. The state object will be included in the state snapshot, so it
     * survives job restarts. For this reason the object must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object
     * has a timestamp attached to it: the top timestamp of any event with the
     * same key seen so far. Upon seeing another event, Jet compares the state
     * timestamp with the current watermark. If it is less than {@code wm - ttl},
     * it discards the state object and creates a new one before processing the
     * event.
     * <p>
     * Sample usage:
     * <pre>{<code
     *
     * }</pre>
     *
     * @param createFn the function that returns the state object
     * @param flatMapFn the function that receives the state object and the input item and
     *                  outputs the result items. It may modify the state object.
     * @param <S>      type of the state object
     * @param <R>      type of the result
     */
    @Nonnull
    <S, R> StreamStage<Entry<K, R>> flatMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    default <S, R> StreamStage<Entry<K, R>> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiFunctionEx<? super S, ? super T, ? extends Traverser<R>> flatMapFn
    ) {
        return flatMapStateful(0, createFn, flatMapFn);
    }

    @Nonnull @Override
    default <A, R> StreamStage<Entry<K, R>> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        return (StreamStage<Entry<K, R>>) GeneralStageWithKey.super.<A, R>rollingAggregate(aggrOp);
    }

    /**
     * Attaches a rolling aggregation stage. This is a special case of
     * {@linkplain #mapStateful stateful mapping} that uses an {@link
     * AggregateOperation1 AggregateOperation}. It passes each input item to
     * the accumulator and outputs the current result of aggregation (as
     * returned by the {@link AggregateOperation1#exportFn() export} primitive).
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<Entry<Color, Long>> aggregated = items
     *         .groupingKey(Item::getColor)
     *         .rollingAggregate(AggregateOperations.counting());
     * }</pre>
     * For example, if your input is {@code {2, 7, 8, -5}}, the output will be
     * {@code {2, 9, 17, 12}}.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * accumulator object stale if its time-to-live has expired. The
     * accumulator object has a timestamp attached to it: the top timestamp of
     * any event with the same key seen so far. Upon seeing another event, Jet
     * compares the accumulator timestamp with the current watermark. If it is
     * less than {@code wm - ttl}, it discards the accumulator object and
     * creates a new one before processing the event.
     * <p>
     * This stage is fault-tolerant and saves its state to the snapshot.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the aggregate operation result
     * @return the newly attached stage
     */
    @Nonnull
    default <A, R> StreamStage<Entry<K, R>> rollingAggregate(
            long ttl,
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        BiConsumer<? super A, ? super T> accumulateFn = aggrOp.accumulateFn();
        Function<? super A, ? extends R> exportFn = aggrOp.exportFn();
        return mapStateful(ttl, aggrOp.createFn(), (acc, item) -> {
            accumulateFn.accept(acc, item);
            return exportFn.apply(acc);
        });
    }

    @Nonnull @Override
    default <V, R> StreamStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStageWithKey.super.<V, R>mapUsingIMap(mapName, mapFn);
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    default <V, R> StreamStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return (StreamStage<R>) GeneralStageWithKey.super.mapUsingIMap(iMap, mapFn);
    }

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriPredicate<? super C, ? super K, ? super T> filterFn
    );

    @Nonnull @Override
    <C> StreamStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>> filterAsyncFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    @Nonnull @Override
    <C, R> StreamStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>> flatMapAsyncFn
    );

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName,
                                               @Nonnull SupplierEx<Processor> procSupplier
    ) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier) {
        return customTransform(stageName, ProcessorMetaSupplier.of(procSupplier));
    }

    @Nonnull @Override
    <R> StreamStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);
}
