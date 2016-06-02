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

import com.hazelcast.jet.api.dag.DAG;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.io.api.tuple.Tuple;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.AbstractIntermediatePipeline;
import com.hazelcast.jet.stream.impl.Pipeline;
import com.hazelcast.jet.stream.impl.processor.TransformProcessor;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.stream.impl.StreamUtil.DEFAULT_TASK_COUNT;
import static com.hazelcast.jet.stream.impl.StreamUtil.defaultFromTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.edgeBuilder;
import static com.hazelcast.jet.stream.impl.StreamUtil.getTupleMapper;
import static com.hazelcast.jet.stream.impl.StreamUtil.vertexBuilder;

public class TransformPipeline extends AbstractIntermediatePipeline {

    private final List<TransformOperation> operations = new ArrayList<>();

    public TransformPipeline(StreamContext context,
                             Pipeline upstream,
                             TransformOperation operation) {
        super(context, upstream.isOrdered(), upstream);
        operations.add(operation);
    }

    @Override
    public Vertex buildDAG(DAG dag, Vertex downstreamVertex, Distributed.Function toTupleMapper) {
        Distributed.Function<Tuple, ?> fromTupleMapper = getTupleMapper(upstream, defaultFromTupleMapper());

        int taskCount = upstream.isOrdered() ? 1 : DEFAULT_TASK_COUNT;
        Vertex vertex = vertexBuilder(TransformProcessor.Factory.class)
                .name("transform")
                .addToDAG(dag)
                .args(fromTupleMapper, toTupleMapper, operations)
                .taskCount(taskCount)
                .build();

        Vertex previous = this.upstream.buildDAG(dag, vertex, toTupleMapper());
        if (previous != vertex) {
            edgeBuilder(previous, vertex)
                    .addToDAG(dag)
                    .build();
        }

        return vertex;
    }

    @Override
    public DistributedStream filter(Distributed.Predicate predicate) {
        operations.add(new TransformOperation(TransformOperation.Type.FILTER, predicate));
        return this;
    }

    @Override
    public DistributedStream map(Distributed.Function mapper) {
        operations.add(new TransformOperation(TransformOperation.Type.MAP, mapper));
        return this;
    }

    @Override
    public DistributedStream flatMap(Distributed.Function mapper) {
        operations.add(new TransformOperation(TransformOperation.Type.FLAT_MAP, mapper));
        return this;
    }


}
