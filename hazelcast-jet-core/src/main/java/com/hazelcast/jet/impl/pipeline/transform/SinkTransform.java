/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.pipeline.SinkImpl;

import javax.annotation.Nonnull;
import java.util.List;

import static com.hazelcast.jet.impl.pipeline.FunctionAdapter.adaptingMetaSupplier;


public class SinkTransform<T> extends AbstractTransform {
    private static final int[] EMPTY_ORDINALS = new int[0];

    @Nonnull
    private ProcessorMetaSupplier metaSupplier;
    @Nonnull
    private final int[] ordinalsToAdapt;

    public SinkTransform(@Nonnull SinkImpl sink, @Nonnull List<Transform> upstream, @Nonnull int[] ordinalsToAdapt) {
        super(sink.name(), upstream);
        this.metaSupplier = sink.metaSupplier();
        this.ordinalsToAdapt = ordinalsToAdapt;
    }

    public SinkTransform(@Nonnull SinkImpl sink, @Nonnull Transform upstream, boolean adaptToJetEvents) {
        super(sink.name(), upstream);
        this.metaSupplier = sink.metaSupplier();
        this.ordinalsToAdapt = adaptToJetEvents ? new int[] {0} : EMPTY_ORDINALS;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(
                this,
                p.uniqueVertexName(name()),
                localParallelism(),
                adaptingMetaSupplier(metaSupplier, ordinalsToAdapt)
        );
        p.addEdges(this, pv.v);
    }
}
