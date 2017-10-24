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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.stream.DistributedCollector;
import com.hazelcast.jet.stream.impl.pipeline.Pipe;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.CombineGroupsP;
import com.hazelcast.jet.stream.impl.processor.GroupAndAccumulateP;

import java.util.stream.Collector;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;

public class GroupingSinkReducer<T, A, K, D, R> implements DistributedCollector.Reducer<T, R> {

    private final String sinkName;
    private final DistributedFunction<JetInstance, ? extends R> toDistributedObject;
    private final DistributedFunction<? super T, ? extends K> classifier;
    private final Collector<? super T, A, D> collector;
    private final ProcessorMetaSupplier metaSupplier;

    public GroupingSinkReducer(String sinkName,
                               DistributedFunction<JetInstance, ? extends R> toDistributedObject,
                               DistributedFunction<? super T, ? extends K> classifier,
                               Collector<? super T, A, D> collector,
                               ProcessorMetaSupplier metaSupplier) {
        this.sinkName = sinkName;
        this.toDistributedObject = toDistributedObject;
        this.classifier = classifier;
        this.collector = collector;
        this.metaSupplier = metaSupplier;
    }

    @Override
    public R reduce(StreamContext context, Pipe<? extends T> upstream) {
        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);
        Vertex merger = dag.newVertex("group-and-accumulate",
                () -> new GroupAndAccumulateP<>(classifier, collector));
        Vertex combiner = dag.newVertex("combine-groups", () -> new CombineGroupsP<>(collector));
        Vertex writer = dag.newVertex(sinkName, metaSupplier);

        dag.edge(between(previous, merger).partitioned(classifier::apply, HASH_CODE))
           .edge(between(merger, combiner).distributed().partitioned(entryKey()))
           .edge(between(combiner, writer));
        executeJob(context, dag);
        return toDistributedObject.apply(context.getJetInstance());
    }
}
