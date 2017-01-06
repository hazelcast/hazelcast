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

import com.hazelcast.jet.stream.impl.processor.SortP;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Vertex;
import java.util.Comparator;

import static com.hazelcast.jet.stream.impl.StreamUtil.randomName;

public class SortPipeline<T> extends AbstractIntermediatePipeline<T, T> {

    private final Comparator<? super T> comparator;

    public SortPipeline(Pipeline<T> upstream, StreamContext context, Comparator<? super T> comparator) {
        super(context, true, upstream);
        this.comparator = comparator;
    }

    @Override
    public Vertex buildDAG(DAG dag) {
        Vertex previous = upstream.buildDAG(dag);
        // required final for lambda variable capture
        final Comparator<? super T> comparator = this.comparator;
        Vertex sorter = new Vertex("sorter-" + randomName(), () -> new SortP<>(comparator)).localParallelism(1);
        dag.addVertex(sorter)
                .addEdge(new Edge(previous, sorter)
                        .distributed()
                        .allToOne()
                );

        return sorter;
    }
}
