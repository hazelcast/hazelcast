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
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.jet.stream.impl.pipeline.Pipeline;
import com.hazelcast.jet.stream.impl.pipeline.StreamContext;
import com.hazelcast.jet.stream.impl.processor.MergeP;

import java.util.function.BinaryOperator;
import java.util.function.Function;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.stream.impl.StreamUtil.executeJob;
import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueMapName;

public class MergingIMapReducer<T, K, V> extends IMapReducer<T, K, V> {

    private final BinaryOperator<V> mergeFunction;

    public MergingIMapReducer(
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            BinaryOperator<V> mergeFunction
    ) {
        this(uniqueMapName(), keyMapper, valueMapper, mergeFunction);
    }

    private MergingIMapReducer(
            String mapName,
            Function<? super T, ? extends K> keyMapper,
            Function<? super T, ? extends V> valueMapper,
            BinaryOperator<V> mergeFunction
    ) {
        super(mapName, keyMapper, valueMapper);
        this.mergeFunction = mergeFunction;
    }

    @Override
    public IStreamMap<K, V> reduce(StreamContext context, Pipeline<? extends T> upstream) {
        IStreamMap<K, V> target = getTarget(context.getJetInstance());
        DAG dag = new DAG();
        Vertex previous = upstream.buildDAG(dag);

        Vertex merge = dag.newVertex("merge-local",
                () -> new MergeP<>(keyMapper, valueMapper, mergeFunction));
        Vertex combine = dag.newVertex("merge-distributed",
                () -> new MergeP<T, K, V>(null, null, mergeFunction));
        Vertex writer = dag.newVertex("write-map-" + mapName, Processors.writeMap(mapName));

        dag.edge(between(previous, merge).partitioned(keyMapper::apply, HASH_CODE))
           .edge(between(merge, combine).distributed().partitioned(entryKey()))
           .edge(between(combine, writer));
        executeJob(context, dag);
        return target;
    }
}
