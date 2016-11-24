/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AccumulatorProcessor;
import com.hazelcast.jet.stream.impl.processor.CombinerProcessor;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.Processors;
import com.hazelcast.jet2.SimpleProcessorSupplier;
import com.hazelcast.jet2.Vertex;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

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
        SimpleProcessorSupplier supplier = () -> new CombinerProcessor<>(combiner, Distributed.Function.<T>identity());
        Vertex combinerVertex = new Vertex(randomName(), supplier).parallelism(1);
        dag.addVertex(combinerVertex);
        dag.addEdge(new Edge(accumulatorVertex, combinerVertex)
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
        String listName = randomName(LIST_PREFIX);
        Vertex writer = new Vertex(randomName(), Processors.listWriter(listName));
        dag.addVertex(writer).addEdge(new Edge(combiner, writer));
        IList<T> list = context.getHazelcastInstance().getList(listName);
        executeJob(context, dag);
        if (list.isEmpty()) {
            list.destroy();
            return Optional.empty();
        }
        T result = list.get(0);
        list.destroy();
        return Optional.of(result);
    }


    private <T, U> Vertex buildMappingAccumulator(DAG dag,
                                                  Pipeline<? extends T> upstream, U identity,
                                                  BiFunction<U, ? super T, U> accumulator) {


        Vertex accumulatorVertex = new Vertex(randomName(), () -> new AccumulatorProcessor<>(accumulator, identity));
        dag.addVertex(accumulatorVertex);
        Vertex previous = upstream.buildDAG(dag);
        if (previous != accumulatorVertex) {
            dag.addEdge(new Edge(previous, accumulatorVertex));
        }
        return accumulatorVertex;
    }

    private <T> Vertex buildAccumulator(
            DAG dag, Pipeline<? extends T> upstream, BinaryOperator<T> accumulator, T identity
    ) {
        Vertex accumulatorVertex = getAccumulatorVertex(accumulator, identity);
        dag.addVertex(accumulatorVertex);

        Vertex previous = upstream.buildDAG(dag);
        if (previous != accumulatorVertex) {
            dag.addEdge(new Edge(previous, accumulatorVertex));
        }
        return accumulatorVertex;
    }

    private <T> Vertex getAccumulatorVertex(
            BinaryOperator<T> accumulator, T identity
    ) {
        return identity != null
                ?
                new Vertex(randomName(), () -> new AccumulatorProcessor<>(accumulator, identity))
                :
                new Vertex(randomName(), () -> new CombinerProcessor<>(accumulator, Distributed.Function.identity()));
    }

}

