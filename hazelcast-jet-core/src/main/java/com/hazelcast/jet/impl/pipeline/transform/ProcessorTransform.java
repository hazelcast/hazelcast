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

import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

/**
 * A unary transform constructed directly from a provided Core API
 * processor supplier.
 */
public class ProcessorTransform extends AbstractTransform {
    @Nonnull
    public final DistributedSupplier<Processor> procSupplier;

    public ProcessorTransform(
            @Nonnull Transform upstream,
            @Nonnull String name,
            @Nonnull DistributedSupplier<Processor> procSupplier
    ) {
        super(name, upstream);
        this.procSupplier = procSupplier;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(), procSupplier);
        p.addEdges(this, pv.v);
    }
}
