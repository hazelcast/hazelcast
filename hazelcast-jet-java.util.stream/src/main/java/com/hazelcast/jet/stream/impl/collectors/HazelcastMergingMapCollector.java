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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.impl.dag.DAGImpl;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.strategy.ProcessingStrategy;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.MergeProcessor;

import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.hazelcast.jet.stream.impl.StreamUtil.MAP_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeApplication;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class HazelcastMergingMapCollector<T, K, V> extends HazelcastMapCollector<T, K, V> {

    private final BinaryOperator<V> mergeFunction;

    public HazelcastMergingMapCollector(Function<? super T, ? extends K> keyMapper,
                                        Function<? super T, ? extends V> valueMapper,
                                        BinaryOperator<V> mergeFunction) {
        this(randomName(MAP_PREFIX), keyMapper, valueMapper, mergeFunction);
    }

    public HazelcastMergingMapCollector(String mapName, Function<? super T, ? extends K> keyMapper,
                                        Function<? super T, ? extends V> valueMapper, BinaryOperator<V> mergeFunction) {
        super(mapName, keyMapper, valueMapper);

        this.mergeFunction = mergeFunction;
    }

    @Override
    public IMap<K, V> collect(StreamContext context, Pipeline<? extends T> upstream) {
        IMap<K, V> target = getTarget(context.getHazelcastInstance());
        DAGImpl dag = new DAGImpl();
        Vertex previous = upstream.buildDAG(dag, null, toTupleMapper());

        Vertex merger = vertexBuilder(MergeProcessor.Factory.class)
                .name("accumulator")
                .addToDAG(dag)
                .args(mergeFunction)
                .build();

        edgeBuilder(previous, merger)
                .addToDAG(dag)
                .processingStrategy(ProcessingStrategy.PARTITIONING)
                .build();

        Vertex combiner = vertexBuilder(MergeProcessor.Factory.class)
                .name("merger")
                .addToDAG(dag)
                .args(mergeFunction)
                .build();

        edgeBuilder(merger, combiner)
                .addToDAG(dag)
                .shuffling(true)
                .processingStrategy(ProcessingStrategy.PARTITIONING)
                .build();

        combiner.addSinkMap(mapName);
        executeApplication(context, dag);
        return target;
    }
}
