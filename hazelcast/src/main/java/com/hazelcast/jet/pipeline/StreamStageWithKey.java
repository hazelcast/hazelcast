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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.function.BiPredicateEx;
import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.SupplierEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.internal.util.MapUtil.entry;


/**
 * An intermediate step while constructing a pipeline transform that
 * involves a grouping key, such as windowed group-and-aggregate. Some
 * transforms use the grouping key only to partition the stream (such as
 * {@link #mapUsingIMap}).
 *
 * @param <T> type of the stream items
 * @param <K> type of the key
 *
 * @since Jet 3.0
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
     * survives job restarts. For this reason it must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object for
     * a given key has a timestamp attached to it: the top timestamp of any
     * event with that key seen so far. Whenever the watermark advances, Jet
     * discards all state objects with a timestamp less than {@code wm - ttl}.
     * Just before discarding the state object, Jet calls {@code onEvictFn} on
     * it. The function can return an output item that will be emitted, or
     * {@code null} if it doesn't need to emit an item. If TTL is used, Jet
     * also drops late events; otherwise, all events are processed.
     * <p>
     * This sample takes a stream of pairs {@code (serverId, latency)}
     * representing the latencies of serving individual requests and keeps
     * track, separately for each server, of the total latency accumulated over
     * individual sessions &mdash; bursts of server activity separated
     * by quiet periods of one minute or more. For each input item it outputs
     * the accumulated latency so far and when a session ends, it outputs a
     * special entry that reports the total latency for that session.
     * <pre>{@code
     * StreamStage<Entry<String, Long>> latencies = null;
     * StreamStage<Entry<String, Long>> cumulativeLatencies = latencies
     *         .groupingKey(Entry::getKey)
     *         .mapStateful(
     *                 MINUTES.toMillis(1),
     *                 LongAccumulator::new,
     *                 (sum, key, entry) -> {
     *                     sum.add(entry.getValue());
     *                     return entry(key, sum.get());
     *                 },
     *                 (sum, key, time) -> entry(String.format(
     *                         "%s:totalForSession:%d", key, time), sum.get())
     *         );
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param ttl        time-to-live for each state object, disabled if zero or less
     * @param createFn   function that returns the state object
     * @param mapFn      function that receives the state object and the input item and
     *                   outputs the result item. It may modify the state object.
     * @param onEvictFn  function that Jet calls when evicting a state object
     *
     * @param <S>        type of the state object
     * @param <R>        type of the result
     */
    @Nonnull
    <S, R> StreamStage<R> mapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    /**
     * Attaches a stage that performs a stateful filtering operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item to {@code filterFn}, which can update
     * the object's state. For each grouping key there's a separate state
     * object. The state object will be included in the state snapshot, so it
     * survives job restarts. For this reason it must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object
     * has a timestamp attached to it: the top timestamp of any event with the
     * same key seen so far. Upon seeing another event, Jet compares the state
     * timestamp with the current watermark. If it is less than {@code wm - ttl},
     * it discards the state object and creates a new one before processing the
     * event. If TTL is used, Jet also drops late events; otherwise, all events
     * are processed.
     * <p>
     * This sample receives a stream of pairs {@code (serverId, requestLatency)}
     * that represent the latencies of individual requests served by a cluster
     * of servers. It emits the record-breaking (worst so far) latencies for
     * each server independently and resets the score after one minute of
     * inactivity on a given server.
     * <pre>{@code
     * StreamStage<Entry<String, Long>> latencies;
     * StreamStage<Entry<String, Long>> topLatencies = latencies
     *         .groupingKey(Entry::getKey)
     *         .filterStateful(
     *                 MINUTES.toMillis(1),
     *                 LongAccumulator::new,
     *                 (topLatencyState, entry) -> {
     *                     long currLatency = entry.getValue();
     *                     long topLatency = topLatencyState.get();
     *                     topLatencyState.set(Math.max(currLatency, topLatency));
     *                     return currLatency > topLatency;
     *                 }
     *         );
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param ttl      time-to-live for each state object, disabled if zero or less
     * @param createFn function that returns the state object
     * @param filterFn predicate that receives the state object and the input item and
     *                 outputs a boolean value. It may modify the state object.
     * @param <S>      type of the state object
     */
    @Nonnull
    <S> StreamStage<T> filterStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    @Nonnull @Override
    default <S> StreamStage<T> filterStateful(
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
     * survives job restarts. For this reason it must be serializable.
     * <p>
     * If the given {@code ttl} is greater than zero, Jet will consider the
     * state object stale if its time-to-live has expired. The state object for
     * a given key has a timestamp attached to it: the top timestamp of any
     * event with that key seen so far. Whenever the watermark advances, Jet
     * discards all state objects with a timestamp less than {@code wm - ttl}.
     * Just before discarding the state object, Jet calls {@code onEvictFn} on
     * it. The function returns a traverser over the items it wants to emit, or
     * it can return an {@linkplain Traversers#empty() empty traverser}. If TTL
     * is used, Jet also drops late events; otherwise, all events are
     * processed.
     * <p>
     * This sample groups a stream of strings by length and inserts punctuation
     * (a special string) after every 10th string in each group, or after one
     * minute elapses without further input for a given key:
     * <pre>{@code
     * StreamStage<String> punctuated = input
     *         .groupingKey(String::length)
     *         .flatMapStateful(
     *                 MINUTES.toMillis(1),
     *                 LongAccumulator::new,
     *                 (counter, key, item) -> {
     *                     counter.add(1);
     *                     return counter.get() % 10 == 0
     *                             ? Traversers.traverseItems("punctuation" + key, item)
     *                             : Traversers.singleton(item);
     *                 },
     *                 (counter, key, wm) -> Traversers.singleton("punctuation" + key)
     *         );
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @param ttl        time-to-live for each state object, disabled if zero or less
     * @param createFn   function that returns the state object
     * @param flatMapFn  function that receives the state object and the input item and
     *                   outputs the result items. It may modify the state object.
     * @param onEvictFn  function that Jet calls when evicting a state object
     *
     * @param <S>        type of the state object
     * @param <R>        type of the result
     */
    @Nonnull
    <S, R> StreamStage<R> flatMapStateful(
            long ttl,
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn,
            @Nonnull TriFunction<? super S, ? super K, ? super Long, ? extends Traverser<R>> onEvictFn
    );

    @Nonnull @Override
     <S, R> StreamStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    );

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
     * This sample takes a stream of items and gives rolling counts of items of
     * each color:
     * <pre>{@code
     * StreamStage<Entry<Color, Long>> aggregated = items
     *         .groupingKey(Item::getColor)
     *         .rollingAggregate(AggregateOperations.counting());
     * }</pre>
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
        FunctionEx<? super T, ? extends K> keyFn = keyFn();
        return mapStateful(ttl, aggrOp.createFn(), (acc, key, item) -> {
            accumulateFn.accept(acc, item);
            return entry(keyFn.apply(item), exportFn.apply(acc));
        }, (state, key, wm) -> null);
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
    <S, R> StreamStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    @Nonnull @Override
    default <S, R> StreamStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return (StreamStage<R>) GeneralStageWithKey.super.mapUsingServiceAsync(serviceFactory, mapAsyncFn);
    }

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull TriFunction<? super S, ? super List<K>, ? super List<T>,
                    ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    @Nonnull @Override
    <S> StreamStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    );

    @Nonnull @Override
    <S, R> StreamStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    );

    @Nonnull @Override
    default <R> StreamStage<R> customTransform(
            @Nonnull String stageName,
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
