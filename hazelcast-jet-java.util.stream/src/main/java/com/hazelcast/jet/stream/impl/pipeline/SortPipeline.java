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

package com.hazelcast.jet.stream.impl.pipeline;

import com.hazelcast.jet.strategy.IListBasedShufflingStrategy;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.SourcePipeline;
import com.hazelcast.jet.stream.impl.processor.EmptyProcessor;
import com.hazelcast.jet.stream.impl.processor.SortProcessor;

import java.util.Comparator;

import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class SortPipeline<T> extends AbstractIntermediatePipeline<T, T> {

    private final Comparator<? super T> comparator;

    public SortPipeline(Pipeline<T> upstream,
                        StreamContext context, Comparator<? super T> comparator) {
        super(context, true, upstream);
        this.comparator = comparator;
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Tuple> toTupleMapper) {
        boolean isFirstVertex = upstream instanceof SourcePipeline;
        Distributed.Function<Tuple, T> fromTupleMapper = getTupleMapper(upstream, defaultFromTupleMapper());
        Vertex vertex = vertexBuilder(SortProcessor.class)
                .name("sorter")
                .addToDAG(dag)
                .args(fromTupleMapper, toTupleMapper, comparator)
                .taskCount(1)
                .build();

        Vertex previous;
        if (isFirstVertex) {
            Vertex passthrough = vertexBuilder(EmptyProcessor.class)
                    .name("passthrough")
                    .addToDAG(dag)
                    .build();
            previous = this.upstream.buildDAG(dag, passthrough, toTupleMapper());
        } else {
            previous = this.upstream.buildDAG(dag, vertex, toTupleMapper());
        }

        edgeBuilder(previous, vertex)
                .addToDAG(dag)
                .shuffling(true)
                .shufflingStrategy(new IListBasedShufflingStrategy(randomName()))
                .build();

        return vertex;
    }
}
