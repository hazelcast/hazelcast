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
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.function.TriPredicate;
import com.hazelcast.jet.impl.pipeline.ComputeStageImplBase;
import com.hazelcast.map.IMap;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.hazelcast.internal.util.MapUtil.entry;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_MAX_CONCURRENT_OPS;
import static com.hazelcast.jet.pipeline.GeneralStage.DEFAULT_PRESERVE_ORDER;

/**
 * An intermediate step when constructing a group-and-aggregate pipeline
 * stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the stream item
 * @param <K> type of the grouping key
 *
 * @since Jet 3.0
 */
public interface GeneralStageWithKey<T, K> {

    /**
     * Returns the function that extracts the key from stream items. The
     * purpose of the key varies with the operation you apply.
     */
    @Nonnull
    FunctionEx<? super T, ? extends K> keyFn();

    /**
     * Attaches a stage that performs a stateful mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item and its key to {@code mapFn}, which
     * can update the object's state. For each grouping key there's a separate
     * state object. The state object will be included in the state snapshot,
     * so it survives job restarts. For this reason it must be serializable.
     * <p>
     * This sample takes a stream of pairs {@code (serverId, latency)}
     * representing the latencies of serving individual requests and outputs
     * the cumulative latency of all handled requests so far, for each
     * server separately:
     * <pre>{@code
     * GeneralStage<Entry<String, Long>> latencies;
     * GeneralStage<Entry<String, Long>> cumulativeLatencies = latencies
     *         .groupingKey(Entry::getKey)
     *         .mapStateful(
     *                 LongAccumulator::new,
     *                 (sum, key, entry) -> {
     *                     sum.add(entry.getValue());
     *                     return entry(key, sum.get());
     *                 }
     *         );
     * }</pre>
     * This code has the same result as {@link #rollingAggregate
     * latencies.groupingKey(Entry::getKey).rollingAggregate(summing())}.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @param createFn function that returns the state object
     * @param mapFn    function that receives the state object and the input item and
     *                 outputs the result item. It may modify the state object. It must be
     *                 stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @param <S>      type of the state object
     * @param <R>      type of the result
     */
    @Nonnull
    <S, R> GeneralStage<R> mapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    /**
     * Attaches a stage that performs a stateful filtering operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item and its key to {@code filterFn}, which
     * can update the object's state. For each grouping key there's a separate
     * state object. The state object will be included in the state snapshot,
     * so it survives job restarts. For this reason it must be serializable.
     * <p>
     * This sample groups a stream of strings by length and decimates each
     * group (throws out every 10th string of each length):
     * <pre>{@code
     * GeneralStage<String> decimated = input
     *         .groupingKey(String::length)
     *         .filterStateful(
     *                 LongAccumulator::new,
     *                 (counter, item) -> {
     *                     counter.add(1);
     *                     return counter.get() % 10 != 0;
     *                 }
     *         );
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @param createFn function that returns the state object
     * @param filterFn predicate that receives the state object and the input item and
     *                 outputs a boolean value. It may modify the state object.
     * @param <S>      type of the state object
     */
    @Nonnull
    <S> GeneralStage<T> filterStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull BiPredicateEx<? super S, ? super T> filterFn
    );

    /**
     * Attaches a stage that performs a stateful flat-mapping operation. {@code
     * createFn} returns the object that holds the state. Jet passes this
     * object along with each input item and its key to {@code flatMapFn},
     * which can update the object's state. For each grouping key there's a
     * separate state object. The state object will be included in the state
     * snapshot, so it survives job restarts. For this reason it must be
     * serializable.
     * <p>
     * This sample groups a stream of strings by length and inserts punctuation
     * (a special string) after every 10th string in each group:
     * <pre>{@code
     * GeneralStage<String> punctuated = input
     *         .groupingKey(String::length)
     *         .flatMapStateful(
     *                 LongAccumulator::new,
     *                 (counter, key, item) -> {
     *                     counter.add(1);
     *                     return counter.get() % 10 == 0
     *                             ? Traversers.traverseItems("punctuation" + key, item)
     *                             : Traversers.singleton(item);
     *                 }
     *         );
     * }</pre>
     * <p>
     * The given functions must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @param createFn  function that returns the state object
     * @param flatMapFn function that receives the state object and the input item and
     *                  outputs the result items. It may modify the state
     *                  object. It must not return null traverser, but can
     *                  return an {@linkplain Traversers#empty() empty traverser}.
     * @param <S>       type of the state object
     * @param <R>       type of the result
     */
    @Nonnull
    <S, R> GeneralStage<R> flatMapStateful(
            @Nonnull SupplierEx<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
    );

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
     * This stage is fault-tolerant and saves its state to the snapshot.
     * <p>
     * This operation is subject to memory limits. See {@link
     * InstanceConfig#setMaxProcessorAccumulatedRecords(long)} for more
     * information.
     *
     * @param aggrOp the aggregate operation to perform
     * @param <R> type of the aggregate operation result
     * @return the newly attached stage
     */
    @Nonnull
    default <A, R> GeneralStage<Entry<K, R>> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, A, ? extends R> aggrOp
    ) {
        BiConsumer<? super A, ? super T> accumulateFn = aggrOp.accumulateFn();
        Function<? super A, ? extends R> exportFn = aggrOp.exportFn();
        return mapStateful(aggrOp.createFn(), (acc, key, item) -> {
            accumulateFn.accept(acc, item);
            return entry(key, exportFn.apply(acc));
        });
    }

    /**
     * Attaches a mapping stage which applies the given function to each input
     * item independently and emits the function's result as the output item.
     * The mapping function receives another parameter, the service object,
     * which Jet will create using the supplied {@code serviceFactory}. If the
     * mapping result is {@code null}, it emits nothing. Therefore this stage
     * can be used to implement filtering semantics as well.
     * <p>
     * Jet uses the {@linkplain #keyFn() key-extracting function} specified on
     * this stage for partitioning: all the items with the same key will see
     * the same service instance (but note that the same instance serves many
     * keys). One case where this is useful is fetching data from an external
     * system because you can use a near-cache without duplicating the cached
     * data.
     * <p>
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingService(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          (reg, key, item) -> item.setDetail(reg.fetchDetail(key))
     *      );
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <S> type of service object
     * @param <R> the result type of the mapping function
     * @param serviceFactory the service factory
     * @param mapFn a mapping function. It must be stateless. It must be
     *     {@linkplain ServiceFactory#isCooperative() cooperative}, if the service
     *     is cooperative.
     * @return the newly attached stage
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> mapFn
    );

    /**
     * Asynchronous version of {@link #mapUsingService}: the {@code mapAsyncFn}
     * returns a {@code CompletableFuture<R>} instead of just {@code R}.
     * <p>
     * Uses default values for some extra parameters, so the maximum number
     * of concurrent async operations per processor will be limited to
     * {@value GeneralStage#DEFAULT_MAX_CONCURRENT_OPS} and
     * whether or not the order of input items should be preserved will be
     * {@value GeneralStage#DEFAULT_PRESERVE_ORDER}.
     * <p>
     * The function can return a null future or the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingServiceAsync(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          (reg, key, item) -> reg.fetchDetailAsync(key).thenApply(item::setDetail)
     *      );
     * }</pre>
     * The latency of the async call will add to the total latency of the
     * output.
     *
     * @param <S> type of service object
     * @param <R> the future's result type of the mapping function
     * @param serviceFactory the service factory
     * @param mapAsyncFn a mapping function. Can map to null (return a null
     *     future). It must be stateless and {@linkplain Processor#isCooperative()
     *     cooperative}.
     * @return the newly attached stage
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    default <S, R> GeneralStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    ) {
        return mapUsingServiceAsync(serviceFactory, DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER, mapAsyncFn);
    }

    /**
     * Asynchronous version of {@link #mapUsingService}: the {@code mapAsyncFn}
     * returns a {@code CompletableFuture<R>} instead of just {@code R}.
     * <p>
     * The function can return a null future or the future can return a null
     * result: in both cases it will act just like a filter.
     * <p>
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingServiceAsync(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          16,
     *          true,
     *          (reg, key, item) -> reg.fetchDetailAsync(key).thenApply(item::setDetail)
     *      );
     * }</pre>
     * The latency of the async call will add to the total latency of the
     * output.
     *
     * @param <S> type of service object
     * @param <R> the future's result type of the mapping function
     * @param serviceFactory the service factory
     * @param maxConcurrentOps maximum number of concurrent async operations per processor
     * @param preserveOrder whether the async responses are ordered or not
     * @param mapAsyncFn a mapping function. Can map to null (return
     *      a null future). It must be stateless and {@linkplain
     *      Processor#isCooperative() cooperative}.
     * @return the newly attached stage
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingServiceAsync(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxConcurrentOps,
            boolean preserveOrder,
            @Nonnull TriFunction<? super S, ? super K, ? super T, CompletableFuture<R>> mapAsyncFn
    );

    /**
     * Batched version of {@link #mapUsingServiceAsync}: {@code mapAsyncFn} takes
     * a list of input items and returns a {@code CompletableFuture<List<R>>}.
     * The size of list is limited by the given {@code maxBatchSize}.
     * <p>
     * This transform can perform filtering by putting {@code null} elements into
     * the output list.
     * <p>
     * The latency of the async call will add to the total latency of the
     * output.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by performing batched lookups from a registry. The max
     * size of the items to lookup is specified as {@code 100}:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingServiceAsyncBatched(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          100,
     *          (reg, itemList) -> reg
     *              .fetchDetailsAsync(itemList.stream().map(Item::getDetailId).collect(Collectors.toList()))
     *              .thenApply(details -> {
     *                  for (int i = 0; i < itemList.size(); i++) {
     *                      itemList.get(i).setDetail(details.get(i));
     *                  }
     *                  return itemList;
     *              })
     *      );
     * }</pre>
     *
     * @param serviceFactory the service factory
     * @param maxBatchSize max size of the input list
     * @param mapAsyncFn a mapping function. It must be stateless. It must be
     *     {@linkplain ServiceFactory#isCooperative() cooperative}, if the service
     *     is cooperative.
     * @param <S> type of service object
     * @param <R> the future result type of the mapping function
     * @return the newly attached stage
     * @since Jet 4.0
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull BiFunctionEx<? super S, ? super List<T>, ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    /**
     * Batched version of {@link #mapUsingServiceAsync}: {@code mapAsyncFn} takes
     * a list of input items (and a list of their corresponding keys) and
     * returns a {@code CompletableFuture<List<R>>}.
     * The sizes of the input lists are identical and are limited by the given
     * {@code maxBatchSize}. The key at index N corresponds to the input item
     * at index N.
     * <p>
     * The number of in-flight batches being completed asynchronously is
     * limited to {@value ComputeStageImplBase#MAX_CONCURRENT_ASYNC_BATCHES}
     * and this mapping operation always preserves the order of input elements.
     * <p>
     * This transform can perform filtering by putting {@code null} elements into
     * the output list.
     * <p>
     * The latency of the async call will add to the total latency of the
     * output.
     * <p>
     * This sample takes a stream of stock items and sets the {@code detail}
     * field on them by performing batched lookups from a registry. The max
     * size of the items to lookup is specified as {@code 100}:
     * <p>
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingServiceAsyncBatched(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          100,
     *          (reg, keyList, itemList) -> reg.fetchDetailsAsync(keyList).thenApply(details -> {
     *              for (int i = 0; i < itemList.size(); i++) {
     *                  itemList.get(i).setDetail(details.get(i));
     *              }
     *              return itemList;
     *          })
     *      );
     * }</pre>
     *
     * @param serviceFactory the service factory
     * @param maxBatchSize max size of the input list
     * @param mapAsyncFn a mapping function. It must be stateless and
     *     {@linkplain Processor#isCooperative() cooperative}.
     * @param <S> type of service object
     * @param <R> the future result type of the mapping function
     * @return the newly attached stage
     * @since Jet 4.0
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S, R> GeneralStage<R> mapUsingServiceAsyncBatched(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            int maxBatchSize,
            @Nonnull TriFunction<? super S, ? super List<K>, ? super List<T>,
                    ? extends CompletableFuture<List<R>>> mapAsyncFn
    );

    /**
     * Attaches a filtering stage which applies the provided predicate function
     * to each input item to decide whether to pass the item to the output or
     * to discard it. The predicate function receives another parameter, the
     * service object, which Jet will create using the supplied {@code
     * serviceFactory}.
     * <p>
     * The number of in-flight batches being completed asynchronously is
     * limited to {@value ComputeStageImplBase#MAX_CONCURRENT_ASYNC_BATCHES}
     * and this mapping operation always preserves the order of input elements.
     * <p>
     * Jet uses the {@linkplain #keyFn() key-extracting function} specified on
     * this stage for partitioning: all the items with the same key will see
     * the same service instance (but note that the same instance serves many
     * keys). One case where this is useful is fetching data from an external
     * system because you can use a near-cache without duplicating the cached
     * data.
     * <p>
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .filterUsingService(
     *          ServiceFactories.sharedService(ctx -> new ItemDetailRegistry()),
     *          (reg, key, item) -> reg.fetchDetail(key).contains("blade")
     *      );
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <S> type of service object
     * @param serviceFactory the service factory
     * @param filterFn a filter predicate function. It must be stateless. It
     *     must be {@linkplain ServiceFactory#isCooperative() cooperative}, if the
     *     service is cooperative.
     * @return the newly attached stage
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S> GeneralStage<T> filterUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriPredicate<? super S, ? super K, ? super T> filterFn
    );

    /**
     * Attaches a flat-mapping stage which applies the supplied function to
     * each input item independently and emits all the items from the
     * {@link Traverser} it returns as the output items. The traverser must
     * be <em>null-terminated</em>. The mapping function receives another
     * parameter, the service object, which Jet will create using the supplied
     * {@code serviceFactory}.
     * <p>
     * Jet uses the {@linkplain #keyFn() key-extracting function} specified on
     * this stage for partitioning: all the items with the same key will see
     * the same service instance (but note that the same instance serves many
     * keys). One case where this is useful is fetching data from an external
     * system because you can use a near-cache without duplicating the cached
     * data.
     * <p>
     * Sample usage:
     * <pre>{@code
     * StreamStage<Part> parts = products
     *     .groupingKey(Product::getId)
     *     .flatMapUsingService(
     *         ServiceFactories.sharedService(ctx -> new PartRegistry()),
     *         (registry, productId, product) -> Traversers.traverseIterable(
     *                 registry.fetchParts(productId))
     *     );
     * }</pre>
     *
     * <h3>Interaction with fault-tolerant unbounded jobs</h3>
     * If you use this stage in a fault-tolerant unbounded job, keep in mind
     * that any state the service object maintains doesn't participate in Jet's
     * fault tolerance protocol. If the state is local, it will be lost after a
     * job restart; if it is saved to some durable storage, the state of that
     * storage won't be rewound to the last checkpoint, so you'll perform
     * duplicate updates.
     *
     * @param <S> type of service object
     * @param <R> type of the output items
     * @param serviceFactory the service factory
     * @param flatMapFn a flatmapping function. It must not return null
     *     traverser, but can return an {@linkplain Traversers#empty() empty
     *     traverser}. It must be stateless. It must be {@linkplain
     *     ServiceFactory#isCooperative() cooperative}, if the service is
     *     cooperative.
     * @return the newly attached stage
     *
     * @deprecated Jet now has first-class support for data rebalancing, see
     * {@link GeneralStage#rebalance()} and {@link GeneralStage#rebalance(FunctionEx)}.
     */
    @Nonnull
    <S, R> GeneralStage<R> flatMapUsingService(
            @Nonnull ServiceFactory<?, S> serviceFactory,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends Traverser<R>> flatMapFn
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
     * <pre>{@code
     * V value = map.get(groupingKey);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingIMap(
     *              "enriching-map",
     *              (Item item, ItemDetail detail) -> item.setDetail(detail)
     *      );
     * }</pre>
     * This stage is similar to {@link GeneralStage#mapUsingIMap(IMap,
     * FunctionEx, BiFunctionEx) stageWithoutKey.mapUsingIMap()},
     * but here Jet knows the key and uses it to partition and distribute the input in order
     * to achieve data locality. The value it fetches from the {@code IMap} is
     * stored on the cluster member where the processing takes place. However,
     * if the map doesn't use the default partitioning strategy, the data
     * locality will be broken.
     *
     * @param mapName name of the {@code IMap}
     * @param mapFn the mapping function. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
     * @param <V> type of the value in the {@code IMap}
     * @param <R> type of the output item
     * @return the newly attached stage
     */
    @Nonnull
    default <V, R> GeneralStage<R> mapUsingIMap(
            @Nonnull String mapName,
            @Nonnull BiFunctionEx<? super T, ? super V, ? extends R> mapFn
    ) {
        return mapUsingServiceAsync(ServiceFactories.<K, V>iMapService(mapName),
                DEFAULT_MAX_CONCURRENT_OPS, DEFAULT_PRESERVE_ORDER,
                (map, key, item) -> map.getAsync(key).toCompletableFuture()
                                       .thenApply(value -> mapFn.apply(item, value)));
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
     * <pre>{@code
     * V value = map.get(groupingKey);
     * return mapFn.apply(item, value);
     * }</pre>
     *
     * Sample usage:
     * <pre>{@code
     * items.groupingKey(Item::getDetailId)
     *      .mapUsingIMap(enrichingMap, (item, detail) -> item.setDetail(detail));
     * }</pre>
     *
     * This stage is similar to {@link GeneralStage#mapUsingIMap(IMap,
     * FunctionEx, BiFunctionEx) stageWithoutKey.mapUsingIMap()},
     * but here Jet knows the key and uses it to partition and distribute the
     * input in order to achieve data locality. The value it fetches from the
     * {@code IMap} is stored on the cluster member where the processing takes
     * place. However, if the map doesn't use the default partitioning strategy,
     * data locality will be broken.
     *
     * @param iMap the {@code IMap} to use as the service
     * @param mapFn the mapping function. It must be stateless and {@linkplain
     *     Processor#isCooperative() cooperative}.
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
