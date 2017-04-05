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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed.BinaryOperator;
import com.hazelcast.jet.Distributed.Function;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.MergeP;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.DistributedFunctions.entryKey;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;

public class MergingSinkReducer<T, K, V, R> extends SinkReducer<T, K, V, R> {

    private final BinaryOperator<V> mergeFunction;

    public MergingSinkReducer(String sinkName,
                              Function<JetInstance, ? extends R> toDistributedObject,
                              Function<? super T, ? extends K> keyMapper,
                              Function<? super T, ? extends V> valueMapper,
                              BinaryOperator<V> mergeFunction,
                              ProcessorSupplier processorSupplier) {
        super(sinkName, toDistributedObject, keyMapper, valueMapper, processorSupplier);
        this.mergeFunction = mergeFunction;
    }

    @Override
    public R reduce(StreamContext context, Pipeline<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);

        Vertex merge = dag.newVertex("merge-local",
                () -> new MergeP<>(keyMapper, valueMapper, mergeFunction));
        Vertex combine = dag.newVertex("merge-distributed",
                () -> new MergeP<T, K, V>(null, null, mergeFunction));
        Vertex writer = dag.newVertex(sinkName, processorSupplier);

        dag.edge(between(previous, merge).partitioned(keyMapper::apply, HASH_CODE))
           .edge(between(merge, combine).distributed().partitioned(entryKey()))
           .edge(between(combine, writer));
        executeJob(context, dag);
        return toDistributedObject.apply(context.getJetInstance());
    }
}
