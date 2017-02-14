/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.jet.stream.impl.processor.TransformP;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueVertexName;

class TransformPipeline extends AbstractIntermediatePipeline {

    private final List<TransformOperation> operations = new ArrayList<>();

    TransformPipeline(StreamContext context, Pipeline upstream, TransformOperation operation) {
        super(context, upstream.isOrdered(), upstream);
        operations.add(operation);
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex previous = upstream.buildDAG(dag);
        // required final for lambda variable capture
        final List<TransformOperation> ops = operations;
        Vertex transform = dag.newVertex(uniqueVertexName("transform"), () -> new TransformP(ops));
        if (upstream.isOrdered()) {
            transform.localParallelism(1);
        }
        dag.edge(between(previous, transform));

        return transform;
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
