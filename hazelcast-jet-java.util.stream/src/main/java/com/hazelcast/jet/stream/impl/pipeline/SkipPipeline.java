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
import com.hazelcast.jet.stream.impl.StreamUtil;
import com.hazelcast.jet.stream.impl.processor.PassthroughProcessor;
import com.hazelcast.jet.stream.impl.processor.SkipProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class SkipPipeline<T> extends AbstractIntermediatePipeline<T, T> {
    private final long skip;

    public SkipPipeline(StreamContext context, Pipeline<T> upstream, long skip) {
        super(context, upstream.isOrdered(), upstream);
        this.skip = skip;
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Tuple> toTupleMapper) {
        Vertex vertex = vertexBuilder(SkipProcessor.Factory.class)
                .name("skip")
                .addToDAG(dag)
                .args(defaultFromTupleMapper(), toTupleMapper, skip)
                .taskCount(1)
                .build();

        Vertex previous = getPreviousVertex(dag, vertex);


        StreamUtil.EdgeBuilder edgeBuilder = edgeBuilder(previous, vertex);

        // if upstream is not ordered, we need to shuffle data to one node
        if (!upstream.isOrdered()) {
            edgeBuilder.shuffling(true)
                    .shufflingStrategy(new IListBasedShufflingStrategy(randomName()));
        }
        edgeBuilder
                .addToDAG(dag)
                .build();

        return vertex;
    }

    private Vertex getPreviousVertex(DAG dag, Vertex vertex) {
        Vertex previous;
        if (upstream instanceof SourcePipeline) {
            SourcePipeline<T> upstream = (SourcePipeline<T>) this.upstream;
            Vertex passthrough = vertexBuilder(PassthroughProcessor.Factory.class)
                    .name("passthrough")
                    .addToDAG(dag)
                    .args(upstream.fromTupleMapper(), toTupleMapper())
                    .build();
            previous = this.upstream.buildDAG(dag, passthrough, toTupleMapper());
        } else {
            previous = this.upstream.buildDAG(dag, vertex, toTupleMapper());
        }
        return previous;
    }
}
