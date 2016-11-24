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
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.GroupingAccumulatorProcessor;
import com.hazelcast.jet.stream.impl.processor.GroupingCombinerProcessor;
import com.hazelcast.jet2.DAG;
import com.hazelcast.jet2.Edge;
import com.hazelcast.jet2.Processors;
import com.hazelcast.jet2.Vertex;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static com.hazelcast.jet.stream.impl.StreamUtil.MAP_PREFIX;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class HazelcastGroupingMapCollector<T, A, K, D> extends AbstractCollector<T, A, IMap<K, D>> {

    private final String mapName;
    private final Function<? super T, ? extends K> classifier;
    private final Collector<? super T, A, D> collector;

    public HazelcastGroupingMapCollector(Distributed.Function<? super T, ? extends K> classifier,
                                         Distributed.Collector<? super T, A, D> collector) {
        this(randomName(MAP_PREFIX), classifier, collector);
    }

    public HazelcastGroupingMapCollector(String mapName, Function<? super T, ? extends K> classifier,
                                         Collector<? super T, A, D> collector) {
        this.mapName = mapName;
        this.classifier = classifier;
        this.collector = collector;
    }

    @Override
    public IMap<K, D> collect(StreamContext context, Pipeline<? extends T> upstream) {
        IMap<K, D> target = context.getHazelcastInstance().getMap(mapName);

        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);
        Vertex merger = new Vertex("grouping-accumulator-" + randomName(),
                () -> new GroupingAccumulatorProcessor<>(classifier, collector));
        Vertex combiner = new Vertex("grouping-combiner-" + randomName(),
                () -> new GroupingCombinerProcessor<>(collector));
        Vertex writer = new Vertex("map-writer-" + mapName, Processors.mapWriter(mapName));

        dag.addVertex(merger)
           .addVertex(combiner)
           .addVertex(writer)
           .addEdge(new Edge(previous, merger).partitioned(item -> classifier.apply((T) item)))
           .addEdge(new Edge(merger, combiner).distributed().partitioned(item -> ((Map.Entry) item).getKey()))
           .addEdge(new Edge(combiner, writer));
        executeJob(context, dag);
        return target;
    }

}
