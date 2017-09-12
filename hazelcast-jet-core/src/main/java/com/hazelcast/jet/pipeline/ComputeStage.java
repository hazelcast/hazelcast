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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.pipeline.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.datamodel.Tuple3;

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
     * Attaches a mapping stage to this stage.
     *
     * @param mapFn the mapping function
     * @param <R> the result type of the mapping function
     */
    <R> ComputeStage<R> map(DistributedFunction<? super E, ? extends R> mapFn);

    /**
     * Attaches a filtering stage to this stage.
     *
     * @param filterFn the filter predicate function
     */
    ComputeStage<E> filter(DistributedPredicate<E> filterFn);

    /**
     * Attaches a flat-mapping stage to this stage.
     *
     * @param flatMapFn the flatmapping function, whose result type is Jet's {@link Traverser}
     * @param <R> the type of items in the result's traversers
     */
    <R> ComputeStage<R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapFn);

    /**
     * Attaches a group-by-key stage to this stage. The items in a group are
     * aggregated using the supplied aggregate operation.
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
     * Attaches to both this and the supplied stage a hash-joining stage.
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
     * Hash-joins this stage with the two supplied stages and returns the
     * resulting stage that is attached to all.
     *
     * @param stage1      the first stage to join
     * @param joinClause1 specifies how to join with {@code stage1}
     * @param stage2      the second stage to join
     * @param joinClause2 specifices how to join with {@code stage2}
     * @param <K1>        the type of key for {@code stage1}
     * @param <E1_IN>     the type of {@code stage1} items
     * @param <E1>        the result type of projection on {@code stage1} items
     * @param <K2>        the type of key for {@code stage2}
     * @param <E2_IN>     the type of {@code stage2} items
     * @param <E2>        the result type of projection on {@code stage2} items
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
     * Co-groups the items of this stage and the items of the supplied stage
     * and returns a stage attached to both.
     *
     * @param thisKeyFn
     * @param s1
     * @param key1Fn
     * @param aggrOp
     * @param <K>
     * @param <A>
     * @param <E1>
     * @param <R>
     * @return
     */
    <K, A, E1, R> ComputeStage<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> s1, DistributedFunction<? super E1, ? extends K> key1Fn,
            AggregateOperation2<E, E1, A, R> aggrOp
    );

    <K, A, E1, E2, R> ComputeStage<Tuple2<K, R>> coGroup(
            DistributedFunction<? super E, ? extends K> thisKeyFn,
            ComputeStage<E1> s1, DistributedFunction<? super E1, ? extends K> key1Fn,
            ComputeStage<E2> s2, DistributedFunction<? super E2, ? extends K> key2Fn,
            AggregateOperation3<E, E1, E2, A, R> aggrOp
    );

    default <K> CoGroupBuilder<K, E> coGroupBuilder(DistributedFunction<? super E, K> thisKeyFn) {
        return new CoGroupBuilder<>(this, thisKeyFn);
    }

    EndStage drainTo(Sink sink);
}
