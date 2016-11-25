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

import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Vertex;

public abstract class AbstractSourcePipeline<E_OUT> extends AbstractPipeline<E_OUT> {

    public AbstractSourcePipeline(StreamContext context) {
        super(context);
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex vertex = new Vertex(getName(), getProducer());
        if (isOrdered()) {
            vertex.parallelism(1);
        }
        dag.addVertex(vertex);
        return vertex;
    }

    protected abstract ProcessorMetaSupplier getProducer();

    protected abstract String getName();
}
