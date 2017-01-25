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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.core.IList;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.BiConsumer;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.Distributed.Supplier;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.CollectorAccumulatorP;
import com.hazelcast.jet.stream.impl.processor.CollectorCombinerP;
import com.hazelcast.jet.stream.impl.processor.CombinerP;

import java.util.Set;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

public class DistributedCollectorImpl<T, A, R> implements DistributedCollector<T, A, R> {

    private final Distributed.Supplier<A> supplier;
    private final Distributed.BiConsumer<A, T> accumulator;
    private final Distributed.BinaryOperator<A> combiner;
    private final Set<Characteristics> characteristics;
    private final Distributed.Function<A, R> finisher;

    public DistributedCollectorImpl(
            Distributed.Supplier<A> supplier, Distributed.BiConsumer<A, T> accumulator,
            Distributed.BinaryOperator<A> combiner, Distributed.Function<A, R> finisher,
            Set<Characteristics> characteristics
    ) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
        this.finisher = finisher;
        this.characteristics = characteristics;
    }

    public DistributedCollectorImpl(
            Distributed.Supplier<A> supplier, Distributed.BiConsumer<A, T> accumulator,
            Distributed.BinaryOperator<A> combiner, Set<Characteristics> characteristics
    ) {
        this(supplier, accumulator, combiner, castingIdentity(), characteristics);
    }

    static <I, R> Distributed.Function<I, R> castingIdentity() {
        return i -> (R) i;
    }

    static <R> R execute(StreamContext context, DAG dag, Vertex combiner) {
        String listName = uniqueListName();
        Vertex writer = dag.newVertex(uniqueVertexName("writer"), Processors.listWriter(listName));
        dag.edge(between(combiner, writer));
        executeJob(context, dag);
        IList<R> list = context.getJetInstance().getList(listName);
        R result = list.get(0);
        list.destroy();
        return result;
    }

    static <T, R> Vertex buildAccumulator(DAG dag, Pipeline<T> upstream, Supplier<R> supplier,
                                          BiConsumer<R, ? super T> accumulator) {
        Vertex accumulatorVertex = dag.newVertex(uniqueVertexName("accumulator"),
                () -> new CollectorAccumulatorP<>(accumulator, supplier));
        if (upstream.isOrdered()) {
            accumulatorVertex.localParallelism(1);
        }
        Vertex previous = upstream.buildDAG(dag);

        if (previous != accumulatorVertex) {
            dag.edge(between(previous, accumulatorVertex));
        }

        return accumulatorVertex;
    }

    static <A, R> Vertex buildCombiner(DAG dag, Vertex accumulatorVertex,
                                       Object combiner, Function<A, R> finisher) {
        Supplier<Processor> processorSupplier = getCombinerSupplier(combiner, finisher);
        Vertex combinerVertex = dag.newVertex(uniqueVertexName("combiner"), processorSupplier).localParallelism(1);
        dag.edge(between(accumulatorVertex, combinerVertex)
                .distributed()
                .allToOne()
        );

        return combinerVertex;
    }

    private static <A, R> Supplier<Processor> getCombinerSupplier(Object combiner, Function<A, R> finisher) {
        if (combiner instanceof BiConsumer) {
            return () -> new CollectorCombinerP((BiConsumer) combiner, finisher);
        } else if (combiner instanceof BinaryOperator) {
            return () -> new CombinerP<>((BinaryOperator) combiner, finisher);
        } else {
            throw new IllegalArgumentException("combiner is of type " + combiner.getClass());
        }
    }

    @Override
    public Distributed.Supplier<A> supplier() {
        return supplier;
    }

    @Override
    public Distributed.BiConsumer<A, T> accumulator() {
        return accumulator;
    }

    @Override
    public Distributed.BinaryOperator<A> combiner() {
        return combiner;
    }

    @Override
    public Distributed.Function<A, R> finisher() {
        return finisher;
    }

    @Override
    public Set<Characteristics> characteristics() {
        return characteristics;
    }

    @Override
    public R collect(StreamContext context, Pipeline<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, supplier(), accumulator());
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, combiner(), finisher);

        return execute(context, dag, combinerVertex);
    }
}
