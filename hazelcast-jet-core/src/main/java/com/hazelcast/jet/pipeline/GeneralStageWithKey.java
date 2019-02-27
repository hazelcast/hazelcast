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
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.BiFunctionEx;
import com.hazelcast.jet.function.SupplierEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.hazelcast.jet.Util.toCompletableFuture;

/**
 * An intermediate step when constructing a group-and-aggregate pipeline
 * stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the stream item
 * @param <K> type of the grouping key
 */
public interface GeneralStageWithKey<T, K> {

    /**
     * Returns the function that extracts the key from stream items. The
     * purpose of the key varies with the operation you apply.
     */
    @Nonnull
    FunctionEx<? super T, ? extends K> keyFn();

    /**
     * Attaches a mapping stage which applies the given function to each input
     * item independently and emits the function's result as the output item.
     * The mapping function receives another parameter, the context object,
     * which Jet will create using the supplied {@code contextFactory}. If the
     * mapping result is {@code null}, it emits nothing. Therefore this stage
     * can be used to implement filtering semantics as well.
     * <p>
     * Jet uses the {@link #keyFn() key-extracting function} specified on this
     * stage for partitioning: all the items with the same key will see the
     * same context instance (but note that the same instance serves many keys).
     * One case where this is useful is fetching data from an external system
     * because you can use a near-cache without duplicating the cached data.
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
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends R> mapFn
    );

    /**
     * Asynchronous version of {@link #mapUsingContext}: the {@code mapAsyncFn}
     * returns a {@code CompletableFuture<R>} instead of just {@code R}.
     * <p>
     * The function can return a null future or the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * The latency of the async call will add to the latency of the items.
     *
     * @param <C> type of context object
     * @param <R> the future's result type of the mapping function
     * @param contextFactory the context factory
     * @param mapAsyncFn a stateless mapping function. Can map to null (return
     *      a null future)
     * @return the newly attached stage
     */
    @Nonnull
    <C, R> GeneralStage<R> mapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. The predicate function receives another parameter, the
     * context object, which Jet will create using the supplied {@code
     * contextFactory}.
     * <p>
     * Jet uses the {@link #keyFn() key-extracting function} specified on this
     * stage for partitioning: all the items with the same key will see the
     * same context instance (but note that the same instance serves many keys).
     * One case where this is useful is fetching data from an external system
     * because you can use a near-cache without duplicating the cached data.
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
            @Nonnull TriPredicate<? super C, ? super K, ? super T> filterFn
    );

    /**
     * Asynchronous version of {@link #filterUsingContext}: the {@code
     * filterAsyncFn} returns a {@code CompletableFuture<Boolean>} instead of
     * just a {@code boolean}.
     * <p>
     * The function must not return a null future.
     * <p>
     * The latency of the async call will add to the latency of the items.
     *
     * @param <C> type of context object
     * @param contextFactory the context factory
     * @param filterAsyncFn a stateless filtering function
     * @return the newly attached stage
     */
    @Nonnull
    <C> GeneralStage<T> filterUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Boolean>> filterAsyncFn
    );

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all the items from the
     * {@link Traverser} it returns as the output items. The traverser must
     * be <em>null-terminated</em>. The mapping function receives another
     * parameter, the context object, which Jet will create using the supplied
     * {@code contextFactory}.
     * <p>
     * Jet uses the {@link #keyFn() key-extracting function} specified on this
     * stage for partitioning: all the items with the same key will see the
     * same context instance (but note that the same instance serves many keys).
     * One case where this is useful is fetching data from an external system
     * because you can use a near-cache without duplicating the cached data.
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
     * @param <R> type of the output items
     * @param contextFactory the context factory
     * @param flatMapFn a stateless flatmapping function
     * @return the newly attached stage
     */
    @Nonnull
    <C, R> GeneralStage<R> flatMapUsingContext(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Asynchronous version of {@link #flatMapUsingContext}: the {@code
     * flatMapAsyncFn} returns a {@code CompletableFuture<Traverser<R>>}
     * instead of just {@code Traverser<R>}.
     * <p>
     * The function can return a null future or the future can return a null
     * traverser: in both cases it will act just like a filter.
     * <p>
     * The latency of the async call will add to the latency of the items.
     *
     * @param <C> type of context object
     * @param <R> the type of the returned stage
     * @param contextFactory the context factory
     * @param flatMapAsyncFn a stateless flatmapping function. Can map to null
     *      (return a null future)
     * @return the newly attached stage
     */
    @Nonnull
    <C, R> GeneralStage<R> flatMapUsingContextAsync(
            @Nonnull ContextFactory<C> contextFactory,
            @Nonnull TriFunction<? super C, ? super K, ? super T, CompletableFuture<Traverser<R>>>
                    flatMapAsyncFn
    );

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * {@code IMap} with the supplied name using the grouping key is performed
     * and the result of the lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as well.
     * <p>
     * The mapping logic is equivalent to:
     *
     * <pre>{@code
     * V value = map.get(groupingKey);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This stage is similar to {@link GeneralStage#mapUsingIMap(IMap,
     * FunctionEx, BiFunctionEx) stageWithoutKey.mapUsingIMap()},
     * but here Jet knows the key and uses it to partition and distribute the input in order
     * to achieve data locality. The value it fetches from the {@code IMap} is
     * stored on the cluster member where the processing takes place. However,
     * if the map doesn't use the default partitioning strategy, the data
     * locality will be broken.
     *
     * @param mapName name of the {@code IMap}
     * @param mapFn the mapping function
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return mapUsingContextAsync(ContextFactories.<K, V>iMapContext(mapName),
                (map, key, item) -> toCompletableFuture(map.getAsync(key)).thenApply(value -> mapFn.apply(item, value)));
    }

    /**
     * Attaches a mapping stage where for each item a lookup in the
     * supplied {@code IMap} using the grouping key is performed
     * and the result of the lookup is merged with the item and emitted.
     * <p>
     * If the result of the mapping is {@code null}, it emits nothing.
     * Therefore this stage can be used to implement filtering semantics as well.
     * <p>
     * The mapping logic is equivalent to:
     *
     * <pre>{@code
     * V value = map.get(groupingKey);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * This stage is similar to {@link GeneralStage#mapUsingIMap(IMap,
     * FunctionEx, BiFunctionEx) stageWithoutKey.mapUsingIMap()},
     * but here Jet knows the key and uses it to partition and distribute the input in order
     * to achieve data locality. The value it fetches from the {@code IMap} is
     * stored on the cluster member where the processing takes place. However,
     * if the map doesn't use the default partitioning strategy, the data
     * locality will be broken.
     *
     * @param iMap the {@code IMap} to use as the context
     * @param mapFn the mapping function
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull IMap<K, V> iMap,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return mapUsingIMap(iMap.getName(), mapFn);
    }

    /**
     * Attaches a rolling aggregation stage. As opposed to regular aggregation,
     * this stage emits the current aggregation result after receiving each
     * item. For example, if your aggregation is <em>summing</em> and the input
     * under a given key is {@code {2, 7, 8, -5}}, the output will be {@code {2,
     * 9, 17, 12}}.
     * <p>
     * This stage is fault-tolerant and saves its state to the snapshot.
     * <p>
     * <strong>NOTE:</strong> if you plan to use an aggregate operation whose
     * result size grows with input size (such as {@code toList} and your data
     * source is unbounded, carefully consider the memory demands this implies.
     * The result will keep growing forever.
     *
     * @param aggrOp the aggregate operation to perform
     * @param mapToOutputFn function that transforms the key and the aggregation result into the
     *                      output item
     * @param <R> type of the aggregate operation result
     * @param <OUT> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    <R, OUT> GeneralStage<OUT> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull BiFunctionEx<? super K, ? super R, ? extends OUT> mapToOutputFn
    );

    /**
     * A shortcut for:
     * <blockquote>
     *     {@link #rollingAggregate(AggregateOperation1,
     *     BiFunctionEx) aggregateRolling(aggrOp, Util::entry)}.
     * </blockquote>
     */
    @Nonnull
    default <R> GeneralStage<Entry<K, R>> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    ) {
        return rollingAggregate(aggrOp, Util::entry);
    }

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s. The inbound edge will be distributed and
     * partitioned using the key function assigned to this stage.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param <R>          the type of the output items
     * @param stageName    a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(@Nonnull String stageName, @Nonnull SupplierEx<Processor> procSupplier);

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s. The inbound edge will be distributed and
     * partitioned using the key function assigned to this stage.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param <R>          the type of the output items
     * @param stageName    a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier);

    /**
     * Attaches a stage with a custom transform based on the provided supplier
     * of Core API {@link Processor}s. The inbound edge will be distributed and
     * partitioned using the key function assigned to this stage.
     * <p>
     * Note that the type parameter of the returned stage is inferred from the
     * call site and not propagated from the processor that will produce the
     * result, so there is no actual type safety provided.
     *
     * @param <R> the type of the output items
     * @param stageName a human-readable name for the custom stage
     * @param procSupplier the supplier of processors
     */
    @Nonnull
    <R> GeneralStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorMetaSupplier procSupplier);
}
