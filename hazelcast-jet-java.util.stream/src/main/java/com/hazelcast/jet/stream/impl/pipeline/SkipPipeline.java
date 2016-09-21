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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.SourcePipeline;
import com.hazelcast.jet.stream.impl.processor.PassthroughProcessor;
import com.hazelcast.jet.stream.impl.processor.SkipProcessor;

import static com.hazelcast.jet.strategy.MemberDistributionStrategy.singlePartition;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromPairMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.newEdge;
import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class SkipPipeline<T> extends AbstractIntermediatePipeline<T, T> {
    private final long skip;

    public SkipPipeline(StreamContext context, Pipeline<T> upstream, long skip) {
        super(context, upstream.isOrdered(), upstream);
        this.skip = skip;
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Pair> toPairMapper) {
        Vertex vertex = vertexBuilder(SkipProcessor.class)
                .name("skip")
                .addToDAG(dag)
                .args(defaultFromPairMapper(), toPairMapper, skip)
                .taskCount(1)
                .build();

        Vertex previous = getPreviousVertex(dag, vertex);


        Edge edge = newEdge(previous, vertex);

        // if upstream is not ordered, we need to shuffle data to one node
        if (!upstream.isOrdered()) {
            edge = edge.distributed(singlePartition(randomName()));
        }
        dag.addEdge(edge);
        return vertex;
    }

    private Vertex getPreviousVertex(DAG dag, Vertex vertex) {
        Vertex previous;
        if (upstream instanceof SourcePipeline) {
            SourcePipeline<T> upstream = (SourcePipeline<T>) this.upstream;
            Vertex passthrough = vertexBuilder(PassthroughProcessor.class)
                    .name("passthrough")
                    .addToDAG(dag)
                    .args(upstream.fromPairMapper(), toPairMapper())
                    .build();
            previous = this.upstream.buildDAG(dag, passthrough, toPairMapper());
        } else {
            previous = this.upstream.buildDAG(dag, vertex, toPairMapper());
        }
        return previous;
    }
}
