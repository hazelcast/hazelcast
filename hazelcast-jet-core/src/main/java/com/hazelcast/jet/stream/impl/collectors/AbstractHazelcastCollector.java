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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;

public abstract class AbstractHazelcastCollector<T, R> extends AbstractCollector<T, Object, R> {

    @Override
    public R collect(StreamContext context, Pipeline<? extends T> upstream) {
        R target = getTarget(context.getJetInstance());
        DAG dag = new DAG();
        Vertex vertex = upstream.buildDAG(dag);
        Vertex writer = new Vertex(getName(), getConsumer());
        if (localParallelism() > 0) {
            writer.localParallelism(localParallelism());
        }
        dag.addVertex(writer).addEdge(new Edge(vertex, writer));
        executeJob(context, dag);
        return target;
    }

    protected abstract R getTarget(JetInstance instance);

    protected abstract ProcessorMetaSupplier getConsumer();

    protected abstract int localParallelism();

    protected abstract String getName();
}
