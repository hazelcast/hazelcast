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
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.sink.ListSink;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.AccumulatorProcessor;
import com.hazelcast.jet.stream.impl.processor.CombinerProcessor;

import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.hazelcast.jet.strategy.MemberDistributionStrategy.singlePartition;
import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromPairMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.getPairMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.newEdge;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

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
        Vertex combinerVertex = vertexBuilder(CombinerProcessor.class)
                .addToDAG(dag)
                .args(defaultFromPairMapper(), toPairMapper())
                .args(combiner, Distributed.Function.<T>identity())
                .taskCount(1)
                .build();
        dag.addEdge(newEdge(accumulatorVertex, combinerVertex)
                .distributed(singlePartition(randomName())));
        return combinerVertex;
    }

    public <T> Optional<T> reduce(Pipeline<T> upstream, BinaryOperator<T> operator) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, operator, Optional.empty());
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, operator);
        return this.<T>execute(dag, combinerVertex);
    }

    public <T> T reduce(Pipeline<T> upstream, T identity, BinaryOperator<T> accumulator) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, accumulator, Optional.of(identity));
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, accumulator);
        return this.<T>execute(dag, combinerVertex).get();
    }

    private <T> Optional<T> execute(DAG dag, Vertex combiner) {
        IList<T> list = context.getHazelcastInstance().getList(randomName(LIST_PREFIX));
        combiner.addSink(new ListSink(list));
        executeJob(context, dag);
        if (list.isEmpty()) {
            list.destroy();
            return Optional.empty();
        }
        T result = list.get(0);
        list.destroy();
        return Optional.of(result);
    }


    private <T, U> Vertex buildMappingAccumulator(
            DAG dag, Pipeline<? extends T> upstream, U identity, BiFunction<U, ? super T, U> accumulator
    ) {
        Distributed.Function<Pair, ? extends T> fromPairMapper = getPairMapper(upstream, defaultFromPairMapper());
        Vertex accumulatorVertex = vertexBuilder(AccumulatorProcessor.class)
                .addToDAG(dag)
                .args(fromPairMapper, toPairMapper())
                .args(accumulator, identity)
                .build();

        Vertex previous = upstream.buildDAG(dag, accumulatorVertex, toPairMapper());
        if (previous != accumulatorVertex) {
            dag.addEdge(newEdge(previous, accumulatorVertex));
        }
        return accumulatorVertex;
    }

    private <T> Vertex buildAccumulator(
            DAG dag, Pipeline<? extends T> upstream, BinaryOperator<T> accumulator, Optional<T> identity
    ) {
        Distributed.Function<Pair, ? extends T> fromPairMapper = getPairMapper(upstream, defaultFromPairMapper());
        Vertex accumulatorVertex = getAccumulatorVertex(accumulator, identity, fromPairMapper);
        dag.addVertex(accumulatorVertex);

        Vertex previous = upstream.buildDAG(dag, accumulatorVertex, toPairMapper());
        if (previous != accumulatorVertex) {
            dag.addEdge(newEdge(previous, accumulatorVertex));
        }
        return accumulatorVertex;
    }

    private <T> Vertex getAccumulatorVertex(
            BinaryOperator<T> accumulator, Optional<T> identity, Function<Pair, ? extends T> fromPairMapper
    ) {
        return identity.isPresent()
                ?
                vertexBuilder(AccumulatorProcessor.class)
                        .args(fromPairMapper, toPairMapper())
                        .args(accumulator, identity.get())
                        .build()
                :
                vertexBuilder(CombinerProcessor.class)
                        .args(fromPairMapper, toPairMapper())
                        .args(accumulator, Distributed.Function.<T>identity())
                        .build();
    }

    private <T, U extends T> Distributed.Function<U, Pair> toPairMapper() {
        return o -> new JetPair<Object, T>(0, o);
    }
}

