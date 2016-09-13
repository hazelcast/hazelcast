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
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.SourcePipeline;
import com.hazelcast.jet.stream.impl.processor.PassthroughProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromPairMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.newEdge;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class UnorderedPipeline<T> extends AbstractIntermediatePipeline<T, T> {
    public UnorderedPipeline(StreamContext context, Pipeline<T> upstream) {
        super(context, false, upstream);
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<T, Pair> toPairMapper) {
        // distribute data to tasks

        // if we are not the first or the last vertex, then let other vertices do the distribution
        if (!(upstream instanceof SourcePipeline) && downstreamVertex != null) {
            return upstream.buildDAG(dag, downstreamVertex, toPairMapper);
        }
        Vertex unordered = vertexBuilder(PassthroughProcessor.class)
                .name("unordered")
                .addToDAG(dag)
                .args(defaultFromPairMapper(), toPairMapper)
                .build();
        Vertex previous = upstream.buildDAG(dag, null, toPairMapper());
        dag.addEdge(newEdge(previous, unordered));
        return unordered;
    }
}
