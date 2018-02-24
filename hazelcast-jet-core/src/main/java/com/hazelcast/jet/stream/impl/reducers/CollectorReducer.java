/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.stream.DistributedCollector.Reducer;
import com.hazelcast.jet.stream.impl.pipeline.Pipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.CollectorAccumulateP;
import com.hazelcast.jet.stream.impl.processor.CollectorCombineP;
import com.hazelcast.jet.stream.impl.processor.CombineP;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.Edge.between;
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

    static <A, R> R execute(StreamContext context, DAG dag, Vertex combiner, Function<A, R> finisher) {
        String listName = uniqueListName();
        Vertex writer = dag.newVertex("write-" + listName, SinkProcessors.writeListP(listName));
        dag.edge(between(combiner, writer));
        executeJob(context, dag);
        IList<A> list = context.getJetInstance().getList(listName);
        A result = list.get(0);
        list.destroy();
        return finisher.apply(result);
    }

    static <T, R> Vertex buildAccumulator(DAG dag, Pipe<T> upstream, Supplier<R> supplier,
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

    static <A, R> Vertex buildCombiner(DAG dag, Vertex accumulatorVertex, Object combiner) {
        DistributedSupplier<Processor> processorSupplier = getCombinerSupplier(combiner);
        Vertex combinerVertex = dag.newVertex("combiner", processorSupplier).localParallelism(1);
        dag.edge(between(accumulatorVertex, combinerVertex)
                .distributed()
                .allToOne()
        );

        return combinerVertex;
    }

    private static DistributedSupplier<Processor> getCombinerSupplier(Object combiner) {
        if (combiner instanceof BiConsumer) {
            return () -> new CollectorCombineP((BiConsumer) combiner);
        } else if (combiner instanceof BinaryOperator) {
            return () -> new CombineP<>((BinaryOperator) combiner);
        } else {
            throw new IllegalArgumentException("combiner is of type " + combiner.getClass());
        }
    }

    @Override
    public R reduce(StreamContext context, Pipe<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex accumulatorVertex = buildAccumulator(dag, upstream, supplier, accumulator);
        Vertex combinerVertex = buildCombiner(dag, accumulatorVertex, combiner);

        return execute(context, dag, combinerVertex, finisher);
    }
}
