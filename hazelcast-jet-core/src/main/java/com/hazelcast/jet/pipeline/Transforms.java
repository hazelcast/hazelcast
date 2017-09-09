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

import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.datamodel.Tag;
import com.hazelcast.jet.pipeline.impl.transform.CoGroupTransform;
import com.hazelcast.jet.pipeline.impl.transform.FilterTransform;
import com.hazelcast.jet.pipeline.impl.transform.FlatMapTransform;
import com.hazelcast.jet.pipeline.impl.transform.GroupByTransform;
import com.hazelcast.jet.pipeline.impl.transform.HashJoinTransform;
import com.hazelcast.jet.pipeline.impl.transform.MapTransform;
import com.hazelcast.jet.pipeline.impl.transform.ProcessorTransform;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map.Entry;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public final class Transforms {

    private Transforms() {
    }

    @Nonnull
    public static <E, R> UnaryTransform<E, R> fromProcessor(
            String transformName, DistributedSupplier<Processor> procSupplier
    ) {
        return new ProcessorTransform<>(transformName, procSupplier);
    }

    @Nonnull
    public static <E, R> UnaryTransform<E, R> map(DistributedFunction<? super E, ? extends R> mapF) {
        return new MapTransform<>(mapF);
    }

    @Nonnull
    public static <E, R> UnaryTransform<E, R> flatMap(DistributedFunction<? super E, Traverser<? extends R>> flatMapF) {
        return new FlatMapTransform<>(flatMapF);
    }

    @Nonnull
    public static <E> UnaryTransform<E, E> filter(DistributedPredicate<? super E> filterF) {
        return new FilterTransform<>(filterF);
    }

    @Nonnull
    public static <E, K, R> UnaryTransform<E, Entry<K, R>> groupBy(
            DistributedFunction<? super E, ? extends K> keyF,
            AggregateOperation1<E, ?, R> aggregation
    ) {
        return new GroupByTransform<>(keyF, aggregation);
    }

    @Nonnull
    public static <K, E0, E1_IN, E1> MultiTransform hashJoin(
            @Nonnull JoinClause<K, E0, E1_IN, E1> joinClause
    ) {
        return new HashJoinTransform<>(singletonList(joinClause), emptyList());
    }

    @Nonnull
    public static <K1, E0, E1_IN, E1, K2, E2_IN, E2> MultiTransform hashJoin(
            @Nonnull JoinClause<K1, E0, E1_IN, E1> joinClause1,
            @Nonnull JoinClause<K2, E0, E2_IN, E2> joinClause2
    ) {
        return new HashJoinTransform<>(asList(joinClause1, joinClause2), emptyList());
    }

    @Nonnull
    public static <E0> MultiTransform hashJoin(
            @Nonnull List<JoinClause<?, E0, ?, ?>> clauses, @Nonnull List<Tag> tags
    ) {
        return new HashJoinTransform<>(clauses, tags);
    }

    @Nonnull
    public static <E0, E1, K, A, R> MultiTransform coGroup(
            DistributedFunction<? super E0, ? extends K> key0F,
            DistributedFunction<? super E1, ? extends K> key1F,
            AggregateOperation2<E0, E1, A, R> aggrOp
    ) {
        return new CoGroupTransform<>(asList(key0F, key1F), aggrOp);
    }

    @Nonnull
    public static <E0, E1, E2, K, A, R> MultiTransform coGroup(
            DistributedFunction<? super E0, ? extends K> key0F,
            DistributedFunction<? super E1, ? extends K> key1F,
            DistributedFunction<? super E2, ? extends K> key2F,
            AggregateOperation3<E0, E1, E2, A, R> aggrOp
    ) {
        return new CoGroupTransform<>(asList(key0F, key1F, key2F), aggrOp);
    }

    @Nonnull
    public static <E0, E1, E2, K, A, R> MultiTransform coGroup(
            List<DistributedFunction<?, ? extends K>> keyFs,
            AggregateOperation<A, R> aggrOp
    ) {
        return new CoGroupTransform<>(keyFs, aggrOp);
    }
}
