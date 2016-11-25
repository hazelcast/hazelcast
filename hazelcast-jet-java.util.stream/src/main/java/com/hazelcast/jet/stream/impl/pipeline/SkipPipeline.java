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

import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.processor.SkipProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;

import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class SkipPipeline<T> extends AbstractIntermediatePipeline<T, T> {
    private final long skip;

    public SkipPipeline(StreamContext context, Pipeline<T> upstream, long skip) {
        super(context, upstream.isOrdered(), upstream);
        this.skip = skip;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex previous = upstream.buildDAG(dag);
        // required final for lambda variable capture
        final long skip = this.skip;
        Vertex skipVertex = new Vertex("skip-" + randomName(), () -> new SkipProcessor(skip)).parallelism(1);
        dag.addVertex(skipVertex);

        Edge edge = new Edge(previous, skipVertex);

        // if upstream is not ordered, we need to shuffle data to one node
        if (!upstream.isOrdered()) {
            edge = edge.distributed().allToOne();
        }
        dag.addEdge(edge);
        return skipVertex;
    }
}
