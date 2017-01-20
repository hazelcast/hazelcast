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
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.SimpleProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AccumulatorP;
import com.hazelcast.jet.stream.impl.processor.CombinerP;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;
import static com.hazelcast.jet.stream.impl.StreamUtil.writerVertexName;

public class Reducer {

    private final StreamContext context;

    public Reducer(StreamContext context) {
        this.context = context;
    }

    public <T, U> U reduce(Pipeline<T> upstream, U identity,
                           BiFunction<U, ? super T, U> accumulator,
                           BinaryOperator<U> combiner) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildMappingAccumulator(dag, upstream, identity, accumulator);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, combiner);

        return this.<U>execute(dag, combinerVertex).get();
    }

    private <T> Vertex buildCombiner(DAG dag, Vertex accumulatorVertex, BinaryOperator<T> combiner) {
        SimpleProcessorSupplier supplier = () -> new CombinerP<>(combiner, Distributed.Function.<T>identity());
        Vertex combinerVertex = dag.newVertex(uniqueVertexName("combiner"), supplier).localParallelism(1);
        dag.edge(between(accumulatorVertex, combinerVertex)
                .distributed()
                .allToOne()
        );
        return combinerVertex;
    }

    public <T> Optional<T> reduce(Pipeline<T> upstream, BinaryOperator<T> operator) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, operator, null);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, operator);
        return this.<T>execute(dag, combinerVertex);
    }

    public <T> T reduce(Pipeline<T> upstream, T identity, BinaryOperator<T> accumulator) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, accumulator, identity);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, accumulator);
        return this.<T>execute(dag, combinerVertex).get();
    }

    private <T> Optional<T> execute(DAG dag, Vertex combiner) {
        String listName = uniqueListName();
        Vertex writer = dag.newVertex(writerVertexName(listName), Processors.listWriter(listName));
        dag.edge(between(combiner, writer));
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
        Vertex acc = dag.newVertex(uniqueVertexName("accumulator"), () -> new AccumulatorP<>(accumulator, identity));
        Vertex previous = upstream.buildDAG(dag);
        if (previous != acc) {
            dag.edge(between(previous, acc));
        }
        return acc;
    }

    private static <T> Vertex buildAccumulator(
            DAG dag, Pipeline<? extends T> upstream, BinaryOperator<T> accumulator, T identity
    ) {
        Vertex accumulatorVertex = getAccumulatorVertex(accumulator, identity);
        dag.vertex(accumulatorVertex);

        Vertex previous = upstream.buildDAG(dag);
        if (previous != accumulatorVertex) {
            dag.edge(between(previous, accumulatorVertex));
        }
        return accumulatorVertex;
    }

    private static <T> Vertex getAccumulatorVertex(
            BinaryOperator<T> accumulator, T identity
    ) {
        return identity != null
                ? new Vertex(uniqueVertexName("accumulator"), () -> new AccumulatorP<>(accumulator, identity))
                : new Vertex(uniqueVertexName("combiner"), () -> new CombinerP<>(
                        accumulator, Distributed.Function.identity()));
    }

}

