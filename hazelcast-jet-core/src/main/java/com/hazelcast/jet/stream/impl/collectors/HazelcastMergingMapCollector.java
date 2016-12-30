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
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.MergeProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;

import java.util.Map;
import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.hazelcast.jet.stream.impl.StreamUtil.MAP_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

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
        IMap<K, V> target = getTarget(context.getJetInstance());
        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);

        Vertex merger = new Vertex("merging-accumulator-" + randomName(),
                () -> new MergeProcessor<T, K, V>(keyMapper,
                        valueMapper, mergeFunction));
        Vertex combiner = new Vertex("merging-combiner-" + randomName(),
                () -> new MergeProcessor<T, K, V>(null, null,
                        mergeFunction));
        Vertex writer = new Vertex("map-writer-" + randomName(), Processors.mapWriter(mapName));

        dag.addVertex(merger)
           .addVertex(combiner)
           .addVertex(writer)
           .addEdge(new Edge(previous, merger).partitionedByKey(item -> keyMapper.apply((T) item)))
           .addEdge(new Edge(merger, combiner).distributed().partitionedByKey(item -> ((Map.Entry) item).getKey()))
           .addEdge(new Edge(combiner, writer));
        executeJob(context, dag);
        return target;
    }
}
