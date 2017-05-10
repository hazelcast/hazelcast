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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AccumulateP;
import com.hazelcast.jet.stream.impl.processor.CombineP;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

public final class Reducers {

    private Reducers() {

    }

    public static class AccumulateCombineWithIdentity<T, U> implements Reducer<T, U> {

        private final U identity;
        private final BiFunction<U, ? super T, U> accumulator;
        private final BinaryOperator<U> combiner;

        public AccumulateCombineWithIdentity(U identity,
                                             BiFunction<U, ? super T, U> accumulator,
                                             BinaryOperator<U> combiner) {
            this.identity = identity;
            this.accumulator = accumulator;
            this.combiner = combiner;
        }

        @Override
        public U reduce(StreamContext context, Pipeline<? extends T> upstream) {
            DAG dag = new DAG();
            Vertex accumulate = buildMappingAccumulator(dag, upstream, identity, accumulator);
            Vertex combine = buildCombiner(dag, accumulate, combiner);

            return Reducers.<U>execute(context, dag, combine).get();
        }
    }

    public static class BinaryAccumulate<T> implements Reducer<T, Optional<T>> {
        private final BinaryOperator<T> accumulator;

        public BinaryAccumulate(BinaryOperator<T> accumulator) {
            this.accumulator = accumulator;
        }

        @Override
        public Optional<T> reduce(StreamContext context, Pipeline<? extends T> upstream) {
            DAG dag = new DAG();
            Vertex accumulate = buildAccumulator(dag, upstream, accumulator, null);
            Vertex combine = buildCombiner(dag, accumulate, accumulator);
            return Reducers.execute(context, dag, combine);
        }
    }

    public static class BinaryAccumulateWithIdentity<T> implements Reducer<T, T> {
        private final T identity;
        private final BinaryOperator<T> accumulator;

        public BinaryAccumulateWithIdentity(T identity, BinaryOperator<T> accumulator) {
            this.identity = identity;
            this.accumulator = accumulator;
        }

        @Override
        public T reduce(StreamContext context, Pipeline<? extends T> upstream) {
            DAG dag = new DAG();
            Vertex accumulate = buildAccumulator(dag, upstream, accumulator, identity);
            Vertex combine = buildCombiner(dag, accumulate, accumulator);
            return Reducers.<T>execute(context, dag, combine).get();
        }
    }


    private static <T> Vertex buildCombiner(DAG dag, Vertex accumulate, BinaryOperator<T> combiner) {
        DistributedSupplier<Processor> supplier = () -> new CombineP<>(combiner, identity());
        Vertex combine = dag.newVertex("combine", supplier).localParallelism(1);
        dag.edge(between(accumulate, combine)
                .distributed()
                .allToOne()
        );
        return combine;
    }

    private static <T> Optional<T> execute(StreamContext context, DAG dag, Vertex combiner) {
        String listName = uniqueListName();
        Vertex writeList = dag.newVertex("write-" + listName, writeList(listName));
        dag.edge(between(combiner, writeList));
        IList<T> list = context.getJetInstance().getList(listName);
        executeJob(context, dag);
        if (list.isEmpty()) {
            list.destroy();
            return Optional.empty();
        }
        T result = list.get(0);
        list.destroy();
        return Optional.of(result);
    }


    private static <T, U> Vertex buildMappingAccumulator(
            DAG dag, Pipeline<? extends T> upstream, U identity, BiFunction<U, ? super T, U> accumulator
    ) {
        Vertex reduce = dag.newVertex("reduce", () -> new AccumulateP<>(accumulator, identity));
        Vertex previous = upstream.buildDAG(dag);
        if (previous != reduce) {
            dag.edge(between(previous, reduce));
        }
        return reduce;
    }

    private static <T> Vertex buildAccumulator(
            DAG dag, Pipeline<? extends T> upstream, BinaryOperator<T> accumulator, T identity
    ) {
        Vertex reduce = reduceVertex(accumulator, identity);
        dag.vertex(reduce);

        Vertex previous = upstream.buildDAG(dag);
        if (previous != reduce) {
            dag.edge(between(previous, reduce));
        }
        return reduce;
    }

    private static <T> Vertex reduceVertex(BinaryOperator<T> accumulator, T identity) {
        return identity != null
                ? new Vertex("reduce", () -> new AccumulateP<>(accumulator, identity))
                : new Vertex("reduce", () -> new CombineP<>(accumulator, identity()));
    }


}
