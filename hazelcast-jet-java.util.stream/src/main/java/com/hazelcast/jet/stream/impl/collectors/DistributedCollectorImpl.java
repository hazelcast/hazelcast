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

package com.hazelcast.jet.stream.impl.collectors;

import com.hazelcast.core.IList;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.data.tuple.JetTuple2;
import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.CollectorAccumulatorProcessor;
import com.hazelcast.jet.stream.impl.processor.CollectorCombinerProcessor;
import com.hazelcast.jet.stream.impl.processor.CombinerProcessor;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.hazelcast.jet.stream.impl.StreamUtil.DEFAULT_TASK_COUNT;
import static com.hazelcast.jet.stream.impl.StreamUtil.LIST_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeApplication;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class DistributedCollectorImpl<T, A, R> implements Distributed.Collector<T, A, R> {

    private final Distributed.Supplier<A> supplier;
    private final Distributed.BiConsumer<A, T> accumulator;
    private final Distributed.BinaryOperator<A> combiner;
    private final Set<Characteristics> characteristics;
    private final Distributed.Function<A, R> finisher;

    public DistributedCollectorImpl(Distributed.Supplier<A> supplier,
                                    Distributed.BiConsumer<A, T> accumulator,
                                    Distributed.BinaryOperator<A> combiner,
                                    Distributed.Function<A, R> finisher,
                                    Set<Characteristics> characteristics) {
        this.supplier = supplier;
        this.accumulator = accumulator;
        this.combiner = combiner;
        this.finisher = finisher;
        this.characteristics = characteristics;
    }

    public DistributedCollectorImpl(Distributed.Supplier<A> supplier,
                                    Distributed.BiConsumer<A, T> accumulator,
                                    Distributed.BinaryOperator<A> combiner,
                                    Set<Characteristics> characteristics) {
        this(supplier, accumulator, combiner, castingIdentity(), characteristics);
    }

    static <I, R> Distributed.Function<I, R> castingIdentity() {
        return i -> (R) i;
    }

    static <R> R execute(StreamContext context, DAG dag, Vertex combiner) {
        IList<R> list = context.getHazelcastInstance().getList(randomName(LIST_PREFIX));
        combiner.addSinkList(list.getName());

        executeApplication(context, dag);
        R result = list.get(0);
        list.destroy();
        return result;
    }

    static <T, R> Vertex buildAccumulator(DAG dag, Pipeline<T> upstream, Supplier<R> supplier,
                                          BiConsumer<R, ? super T> accumulator) {
        Distributed.Function<Tuple, ? extends T> fromTupleMapper = getTupleMapper(upstream, defaultFromTupleMapper());
        int taskCount = upstream.isOrdered() ? 1 : DEFAULT_TASK_COUNT;
        Vertex accumulatorVertex = vertexBuilder(CollectorAccumulatorProcessor.Factory.class)
                .addToDAG(dag)
                .name("accumulator")
                .taskCount(taskCount)
                .args(fromTupleMapper, toTupleMapper())
                .args(accumulator, supplier)
                .build();

        Vertex previous = upstream.buildDAG(dag, accumulatorVertex, toTupleMapper());

        if (previous != accumulatorVertex) {
            edgeBuilder(previous, accumulatorVertex)
                    .addToDAG(dag)
                    .build();
        }

        return accumulatorVertex;
    }

    static <A, R> Vertex buildCombiner(DAG dag, Vertex accumulatorVertex,
                                       Object combiner, Function<A, R> finisher) {
        Class factory = getCombinerClass(combiner);
        Vertex combinerVertex = vertexBuilder(factory)
                .name("combiner")
                .addToDAG(dag)
                .args(defaultFromTupleMapper(), toTupleMapper())
                .args(combiner, finisher)
                .taskCount(1)
                .build();

        edgeBuilder(accumulatorVertex, combinerVertex)
                .addToDAG(dag)
                .shuffling(true)
                .shufflingStrategy(new IListBasedShufflingStrategy(randomName()))
                .build();

        return combinerVertex;
    }

    private static Class getCombinerClass(Object combiner) {
        if (combiner instanceof BiConsumer) {
            return CollectorCombinerProcessor.Factory.class;
        } else if (combiner instanceof BinaryOperator) {
            return CombinerProcessor.Factory.class;
        } else {
            throw new IllegalArgumentException("combiner is of type " + combiner.getClass());
        }
    }

    protected static <T, U extends T> Distributed.Function<U, Tuple> toTupleMapper() {
        return o -> new JetTuple2<Object, T>(0, o);
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
