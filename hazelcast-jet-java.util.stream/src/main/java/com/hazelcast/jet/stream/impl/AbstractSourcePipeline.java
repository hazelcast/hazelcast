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

package com.hazelcast.jet.stream.impl;

import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.PassthroughProcessor;

import static com.hazelcast.jet.stream.impl.StreamUtil.DEFAULT_TASK_COUNT;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public abstract class AbstractSourcePipeline<E_OUT> extends AbstractPipeline<E_OUT> implements SourcePipeline<E_OUT> {

    public AbstractSourcePipeline(StreamContext context) {
        super(context);
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function<E_OUT, Tuple> toTupleMapper) {
        if (downstreamVertex == null) {
            downstreamVertex = vertexBuilder(PassthroughProcessor.Factory.class)
                    .addToDAG(dag)
                    .args(fromTupleMapper(), toTupleMapper)
                    .taskCount(isOrdered() ? 1 : DEFAULT_TASK_COUNT)
                    .build();
        }
        downstreamVertex.addSource(getSourceTap());
        return downstreamVertex;
    }
}
