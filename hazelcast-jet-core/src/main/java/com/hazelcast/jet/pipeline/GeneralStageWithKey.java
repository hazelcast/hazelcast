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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBiPredicate;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.Map.Entry;

/**
 * Represents an intermediate step when constructing a group-and-aggregate
 * pipeline stage. This is the base type for the batch and stream variants.
 *
 * @param <T> type of the stream item
 * @param <K> type of the grouping key
 */
public interface GeneralStageWithKey<T, K> {

    /**
     * Returns the function that extracts the grouping key from stream items.
     * This function will be used in the aggregating stage you are about to
     * construct using this object.
     */
    @Nonnull
    DistributedFunction<? super T, ? extends K> keyFn();

    /**
     * Attaches to this stage a mapping stage, one which applies the supplied
     * function to each input item independently and emits the function's result
     * as the output item. The mapping function receives another parameter, the
     * context object which Jet will create using the supplied {@code
     * contextFactory}.
     * <p>
     * If the mapping result is {@code null}, it emits nothing. Therefore this
     * stage can be used to implement filtering semantics as well.
     *
     * <h3>Note on state saving</h3>
     * Any state you maintain in the context object does not automatically
     * become a part of a fault-tolerant snapshot. If Jet must restore from a
     * snapshot, your state will either be lost (if it was just local state) or
     * not rewound to the checkpoint (if it was stored in some durable
     * storage).
     *
     * <h3>Note on item retention in {@linkplain GeneralStage#addTimestamps
     * jobs with timestamps}</h3>
     *
     * The context should not be used to accumulate stream items and emit the
     * result later. For example to run an async operation for the item and
     * return the result later with next item. This can cause that the
     * watermark to overtake the items and render the items late.
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
     * Attaches to this stage a filtering stage, one which applies the provided
     * predicate function to each input item to decide whether to pass the item
     * to the output or to discard it. The predicate function receives another
     * parameter, the context object which Jet will create using the supplied
     * {@code contextFactory}.
     *
     * <h3>Note on state saving</h3>
     * Any state you maintain in the context object does not automatically
     * become a part of a fault-tolerant snapshot. If Jet must restore from a
     * snapshot, your state will either be lost (if it was just local state) or
     * not rewound to the checkpoint (if it was stored in some durable
     * storage).
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
     * Attaches to this stage a flat-mapping stage, one which applies the
     * supplied function to each input item independently and emits all the
     * items from the {@link Traverser} it returns as the output items. The
     * traverser must be <em>null-terminated</em>. The mapping function
     * receives another parameter, the context object which Jet will create
     * using the supplied {@code contextFactory}.
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
            @Nonnull DistributedBiFunction<? super C, ? super T, ? extends Traverser<? extends R>> flatMapFn
    );

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code IMap} with the supplied name. Jet will use the
     * specified {@linkplain #keyFn() key function} to retrieve a value from
     * the map and pass it to the mapping function you supply, as the second
     * argument.
     * <p>
     * This stage is similar to {@link GeneralStage#mapUsingIMap(String,
     * DistributedBiFunction) stageWithoutKey.mapUsingIMap()}, but here Jet
     * knows the key and uses it to partition and distribute the input in order
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
            @Nonnull DistributedBiFunction<? super T, ? super V, ? extends R> mapFn
    ) {
        DistributedFunction<? super T, ? extends K> keyFn = keyFn();
        return mapUsingContext(ContextFactories.<K, V>iMapContext(mapName),
                (map, item) -> mapFn.apply(item, map.get(keyFn.apply(item))));
    }

    /**
     * Attaches a {@link #mapUsingContext} stage where the context is a
     * Hazelcast {@code IMap}. <strong>It is not necessarily the map you
     * provide here</strong>, but a map with the same name in the Jet cluster
     * that executes the pipeline. Jet will use the specified {@linkplain
     * #keyFn() key function} to retrieve a value from the map and pass it to
     * the mapping function you supply, as the second argument.
     * <p>
     * This stage is similar to {@link GeneralStage#mapUsingIMap(IMap,
     * DistributedBiFunction) stageWithoutKey.mapUsingIMap()}, but here Jet
     * knows the key and uses it to partition and distribute the input in order
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
            @Nonnull DistributedBiFunction<? super T, ? super V, ? extends R> mapFn
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
    @SuppressWarnings("unchecked")
    <R, OUT> GeneralStage<OUT> rollingAggregate(
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp,
            @Nonnull DistributedBiFunction<? super K, ? super R, ? extends OUT> mapToOutputFn
    );

    /**
     * A shortcut for:
     * <blockquote>
     *     {@link #rollingAggregate(AggregateOperation1,
     *     DistributedBiFunction) aggregateRolling(aggrOp, Util::entry)}.
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
     * of Core API {@link Processor}s. To be compatible with the rest of the
     * pipeline, the processor must expect a single inbound edge and
     * arbitrarily many outbound edges, and it must emit the same data to all
     * outbound edges. The inbound edge will be distributed and partitioned
     * using the key function assigned to this stage.
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
    <R> GeneralStage<R> customTransform(@Nonnull String stageName, @Nonnull ProcessorSupplier procSupplier);
}
