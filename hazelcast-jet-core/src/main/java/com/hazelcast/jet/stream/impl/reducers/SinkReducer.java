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

package com.hazelcast.jet.stream.impl.reducers;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.stream.impl.pipeline.Pipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;

public class SinkReducer<T, K, V, R> implements DistributedCollector.Reducer<T, R> {

    final String sinkName;
    final DistributedFunction<JetInstance, ? extends R> toDistributedObject;
    final DistributedFunction<? super T, ? extends K> keyMapper;
    final DistributedFunction<? super T, ? extends V> valueMapper;
    final ProcessorSupplier processorSupplier;

    public SinkReducer(String sinkName,
                       DistributedFunction<JetInstance, ? extends R> toDistributedObject,
                       DistributedFunction<? super T, ? extends K> keyMapper,
                       DistributedFunction<? super T, ? extends V> valueMapper,
                       ProcessorSupplier processorSupplier) {
        this.sinkName = sinkName;
        this.toDistributedObject = toDistributedObject;
        this.keyMapper = keyMapper;
        this.valueMapper = valueMapper;
        this.processorSupplier = processorSupplier;
    }


    @Override
    public R reduce(StreamContext context, Pipe<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);
        Vertex mapper = dag.newVertex("map", Processors.map((T t) -> entry(keyMapper.apply(t), valueMapper.apply(t))));
        Vertex writer = dag.newVertex(sinkName, processorSupplier);

        dag.edge(between(previous, mapper));
        dag.edge(between(mapper, writer));
        executeJob(context, dag);
        return toDistributedObject.apply(context.getJetInstance());
    }
}
