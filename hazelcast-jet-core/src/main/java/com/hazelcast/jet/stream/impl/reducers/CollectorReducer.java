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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.CollectorAccumulateP;
import com.hazelcast.jet.stream.impl.processor.CollectorCombineP;
import com.hazelcast.jet.stream.impl.processor.CombineP;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

/**
 * Performs a reduction using the functions from a {@link java.util.stream.Collector}
 */
public class CollectorReducer<T, A, R> implements Reducer<T, R> {

    private final Supplier<A> supplier;
    private final BiConsumer<A, T> accumulator;
    private final BinaryOperator<A> combiner;
    private final Function<A, R> finisher;

    public CollectorReducer(
            Supplier<A> supplier, BiConsumer<A, T> accumulator,
            BinaryOperator<A> combiner, Function<A, R> finisher) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
        this.finisher = finisher;
    }

    static <R> R execute(StreamContext context, DAG dag, Vertex combiner) {
        String listName = uniqueListName();
        Vertex writer = dag.newVertex("write-" + listName, writeList(listName));
        dag.edge(between(combiner, writer));
        executeJob(context, dag);
        IList<R> list = context.getJetInstance().getList(listName);
        R result = list.get(0);
        list.destroy();
        return result;
    }

    static <T, R> Vertex buildAccumulator(DAG dag, Pipeline<T> upstream, Supplier<R> supplier,
                                          BiConsumer<R, ? super T> accumulator) {
        Vertex accumulatorVertex = dag.newVertex("accumulator",
                () -> new CollectorAccumulateP<>(accumulator, supplier));
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
        Distributed.Supplier<Processor> processorSupplier = getCombinerSupplier(combiner, finisher);
        Vertex combinerVertex = dag.newVertex("combiner", processorSupplier).localParallelism(1);
        dag.edge(between(accumulatorVertex, combinerVertex)
                .distributed()
                .allToOne()
        );

        return combinerVertex;
    }

    private static <A, R> Distributed.Supplier<Processor> getCombinerSupplier(Object combiner, Function<A, R> finisher) {
        if (combiner instanceof BiConsumer) {
            return () -> new CollectorCombineP((BiConsumer) combiner, finisher);
        } else if (combiner instanceof BinaryOperator) {
            return () -> new CombineP<>((BinaryOperator) combiner, finisher);
        } else {
            throw new IllegalArgumentException("combiner is of type " + combiner.getClass());
        }
    }

    @Override
    public R reduce(StreamContext context, Pipeline<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, supplier, accumulator);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, combiner, finisher);

        return execute(context, dag, combinerVertex);
    }
}
