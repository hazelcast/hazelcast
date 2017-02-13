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

package com.hazelcast.jet.stream.impl.terminal;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AccumulateP;
import com.hazelcast.jet.stream.impl.processor.CombineP;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static com.hazelcast.jet.Distributed.Function.identity;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.stream.impl.StreamUtil.checkSerializable;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

public class Reducer {

    private final StreamContext context;

    public Reducer(StreamContext context) {
        this.context = context;
    }

    public <T, U> U reduce(Pipeline<T> upstream, U identity,
                           Distributed.BiFunction<U, ? super T, U> accumulator,
                           Distributed.BinaryOperator<U> combiner) {
        return reduce(upstream, identity, (BiFunction<U, ? super T, U>) accumulator, combiner);
    }

    public <T, U> U reduce(Pipeline<T> upstream, U identity,
                           BiFunction<U, ? super T, U> accumulator,
                           BinaryOperator<U> combiner) {
        checkSerializable(accumulator, "accumulator");
        checkSerializable(combiner, "combiner");
        DAG dag = new DAG();
        Vertex accumulate = buildMappingAccumulator(dag, upstream, identity, accumulator);
        Vertex combine = buildCombiner(dag, accumulate, combiner);

        return this.<U>execute(dag, combine).get();
    }

    private static <T> Vertex buildCombiner(DAG dag, Vertex accumulate, BinaryOperator<T> combiner) {
        Supplier<Processor> supplier = () -> new CombineP<>(combiner, identity());
        Vertex combine = dag.newVertex("combine", supplier).localParallelism(1);
        dag.edge(between(accumulate, combine)
                .distributed()
                .allToOne()
        );
        return combine;
    }

    public <T> Optional<T> reduce(Pipeline<T> upstream, Distributed.BinaryOperator<T> operator) {
        return reduce(upstream, (BinaryOperator<T>) operator);
    }

    public <T> Optional<T> reduce(Pipeline<T> upstream, BinaryOperator<T> operator) {
        checkSerializable(operator, "operator");
        DAG dag = new DAG();
        Vertex accumulate = buildAccumulator(dag, upstream, operator, null);
        Vertex combine = buildCombiner(dag, accumulate, operator);
        return this.<T>execute(dag, combine);
    }

    public <T> T reduce(Pipeline<T> upstream, T identity, Distributed.BinaryOperator<T> accumulator) {
        return reduce(upstream, identity, (BinaryOperator<T>) accumulator);
    }

    public <T> T reduce(Pipeline<T> upstream, T identity, BinaryOperator<T> accumulator) {
        checkSerializable(accumulator, "accumulator");
        DAG dag = new DAG();
        Vertex accumulate = buildAccumulator(dag, upstream, accumulator, identity);
        Vertex combine = buildCombiner(dag, accumulate, accumulator);
        return this.<T>execute(dag, combine).get();
    }

    private <T> Optional<T> execute(DAG dag, Vertex combiner) {
        String listName = uniqueListName();
        Vertex writeList = dag.newVertex("write-list-" + listName, Processors.writeList(listName));
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
