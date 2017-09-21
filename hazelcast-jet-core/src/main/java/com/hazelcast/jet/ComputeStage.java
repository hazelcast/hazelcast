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

package com.hazelcast.jet;

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;

import java.util.Map.Entry;

/**
 * Represents a stage in a distributed computation {@link Pipeline
 * pipeline}. It accepts input from its upstream stages (if any) and passes
 * its output to its downstream stages.
 *
 * @param <E> the type of items coming out of this stage
 */
public interface ComputeStage<E> extends Stage {

    /**
     * Attaches to this stage a mapping stage, one which applies the supplied
     * function to each input item independently and emits the function's
     * result as the output item. Returns the newly attached stage.
     *
     * @param mapFn the mapping function
     * @param <R> the result type of the mapping function
     */
    <R> ComputeStage<R> map(DistributedFunction<? super E, ? extends R> mapFn);

    /**
     * Attaches to this stage a filtering stage, one which applies the provided
     * predicate function to each input item to decide whether to pass the item
     * to the output or to discard it. Returns the newly attached stage.
     *
     * @param filterFn the filter predicate function
     */
    ComputeStage<E> filter(DistributedPredicate<E> filterFn);

    /**
     * Attaches to this stage a flat-mapping stage, one which applies the
     * supplied function to each input item independently and emits all items
     * from the {@link Traverser} it returns as the output items. Returns the
     * newly attached stage.
     *
     * @param flatMapFn the flatmapping function, whose result type is Jet's {@link Traverser}
     * @param <R> the type of items in the result's traversers
     */
    <R> ComputeStage<R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapFn);

    /**
     * Attaches to this stage a group-by-key stage, one which will group all
     * received items by the key returned from the provided key-extracting
     * function. It will apply the provided aggregate operation to the items
     * in each group and emit the result of aggregation per grouping key as
     * the results.
     *
     * @param keyFn the function that extracts the grouping key from an item
     * @param aggrOp the aggregate operation to perform
     * @param <K> the type of key
     * @param <R> the type of the aggregation result
     */
    <K, R> ComputeStage<Entry<K, R>> groupBy(
            DistributedFunction<? super E, ? extends K> keyFn, AggregateOperation1<E, ?, R> aggrOp
    );

    /**
     * Attaches to both this and the supplied stage a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1     the stage to hash-join with this one
     * @param joinClause specifies how to join the two streams
     * @param <K>        the type of the join key
     * @param <E1_IN>     the type of {@code stage1} items
     * @param <E1>       the result type of projection on {@code stage1} items
     */
    <K, E1_IN, E1> ComputeStage<Tuple2<E, E1>> hashJoin(
            ComputeStage<E1_IN> stage1, JoinClause<K, E, E1_IN, E1> joinClause
    );

    /**
     * Attaches to this and the two supplied stages a hash-joining stage and
     * returns it. This stage plays the role of the <em>primary stage</em> in
     * the hash-join. Please refer to the {@link com.hazelcast.jet.pipeline
     * package Javadoc} for a detailed description of the hash-join transform.
     *
     * @param stage1      the first stage to join
     * @param joinClause1 specifies how to join with {@code stage1}
     * @param stage2      the second stage to join
     * @param joinClause2 specifices how to join with {@code stage2}
     * @param <K1>        the type of key for {@code stage1}
     * @param <E1_IN>     the type of {@code stage1} items
     * @param <E1>        the result type of projection of {@code stage1} items
     * @param <K2>        the type of key for {@code stage2}
     * @param <E2_IN>     the type of {@code stage2} items
     * @param <E2>        the result type of projection of {@code stage2} items
     */
    <K1, E1_IN, E1, K2, E2_IN, E2> ComputeStage<Tuple3<E, E1, E2>> hashJoin(
            ComputeStage<E1_IN> stage1, JoinClause<K1, E, E1_IN, E1> joinClause1,
            ComputeStage<E2_IN> stage2, JoinClause<K2, E, E2_IN, E2> joinClause2
    );

    /**
     * Returns a fluent API builder object to construct a hash join operation
     * with any number of contributing stages. This object is mainly intended
     * to build a hash-join of the primary stage with three or more
     * contributing stages. For one or two stages the direct
     * {@code stage.hashJoin(...)} calls should be preferred because they offer
     * more static type safety.
     */
    default HashJoinBuilder<E> hashJoinBuilder() {
        return new HashJoinBuilder<>(this);
    }

    /**
     * Attaches to this and the supplied stage a stage that co-groups their items
     * by a common key and applies the supplied aggregate operation to co-grouped
     * items.
     *
     * @param thisKeyFn a function that extracts the grouping key from this stage's items
     * @param stage1    the stage to co-group with this one
     * @param key1Fn    a function that extracts the grouping key from {@code stage1} items
     * @param aggrOp    the aggregate operation to perform on co-grouped items
     * @param <K>       the type of the grouping key
     * @param <A>       the type of the accumulator
     * @param <E1>      the type of {@code stage1} items
     * @param <R>       the result type of the aggregate operation
     */
    <K, A, E1, R> ComputeStage<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> stage1, DistributedFunction<? super E1, ? extends K> key1Fn,
            AggregateOperation2<E, E1, A, R> aggrOp
    );

    /**
     * Attaches to this and the supplied stages a stage that co-groups their items
     * by a common key and applies the supplied aggregate operation to co-grouped
     * items.
     *
     * @param thisKeyFn a function that extracts the grouping key from this stage's items
     * @param stage1    the first stage to co-group with this one
     * @param key1Fn    a function that extracts the grouping key from {@code stage1} items
     * @param stage2    the second stage to co-group with this one
     * @param key2Fn    a function that extracts the grouping key from {@code stage2} items
     * @param aggrOp    the aggregate operation to perform on co-grouped items
     * @param <K>       the type of the grouping key
     * @param <A>       the type of the accumulator
     * @param <E1>      the type of {@code stage1} items
     * @param <E2>      the type of {@code stage1} items
     * @param <R>       the result type of the aggregate operation
     */
    <K, A, E1, E2, R> ComputeStage<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> stage1, DistributedFunction<? super E1, ? extends K> key1Fn,
            ComputeStage<E2> stage2, DistributedFunction<? super E2, ? extends K> key2Fn,
            AggregateOperation3<E, E1, E2, A, R> aggrOp
    );

    /**
     * Returns a fluent API builder object to construct a co-group operation
     * with any number of contributing stages. This object is mainly intended
     * to build a co-grouping of the primary stage with three or more
     * contributing stages. For one or two stages the direct {@code
     * stage.coGroup(...)} calls should be preferred because they offer more
     * static type safety.
     *
     * @param thisKeyFn a function that extracts the grouping key from this stage's items
     * @param <K>       the type of the grouping key
     */
    default <K> CoGroupBuilder<K, E> coGroupBuilder(DistributedFunction<? super E, K> thisKeyFn) {
        return new CoGroupBuilder<>(this, thisKeyFn);
    }

    /**
     * Attaches to this stage a sink stage, one that accepts data but doesn't
     * emit any. The supplied argument specifies what to do with the received
     * data (typically push it to some outside resource).
     */
    SinkStage drainTo(Sink sink);

    /**
     * Attaches to this stage a stage with a custom transform based on the
     * provided supplier of Core API {@link Processor}s. To be compatible with
     * the rest of the pipeline, the processor must expect a single inbound
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
    <R> ComputeStage<R> customTransform(String stageName, DistributedSupplier<Processor> procSupplier);
}
