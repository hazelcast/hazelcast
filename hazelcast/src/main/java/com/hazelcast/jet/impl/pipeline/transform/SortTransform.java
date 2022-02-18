/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.function.ComparatorEx;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.function.FunctionEx.identity;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.Processors.sortP;


public class SortTransform<T> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    private static final String COLLECT_STAGE_SUFFIX = "-collect";
    private final ComparatorEx<? super T> comparator;

    @SuppressWarnings("unchecked")
    public SortTransform(@Nonnull Transform upstream, @Nullable ComparatorEx<? super T> comparator) {
        super("sort", upstream);
        if (comparator == null) {
            this.comparator = (ComparatorEx<? super T>) ComparatorEx.naturalOrder();
        } else {
            this.comparator = comparator;
        }
    }

    @Override
    public void addToDag(Planner p, Context context) {
        String vertexName = name();
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, p.isPreserveOrder());
        Vertex v1 = p.dag.newVertex(vertexName, sortP(comparator))
                         .localParallelism(determinedLocalParallelism());
        if (p.isPreserveOrder()) {
            p.addEdges(this, v1, Edge::isolated);
        } else {
            p.addEdges(this, v1);
        }
        determinedLocalParallelism(1);
        PlannerVertex pv2 = p.addVertex(this, vertexName + COLLECT_STAGE_SUFFIX, determinedLocalParallelism(),
                ProcessorMetaSupplier.forceTotalParallelismOne(ProcessorSupplier.of(mapP(identity())), vertexName));
        p.dag.edge(between(v1, pv2.v)
                .distributed()
                .allToOne(vertexName)
                .ordered(comparator));
    }
}
