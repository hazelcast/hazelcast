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

package com.hazelcast.jet.impl.pipeline.transform;

import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.processor.Processors.filterP;

public class FilterTransform<T> extends AbstractTransform {
    @Nonnull
    private DistributedPredicate<? super T> filterFn;

    public FilterTransform(
            @Nonnull Transform upstream,
            @Nonnull DistributedPredicate<? super T> filterFn
    ) {
        super("filter", upstream);
        this.filterFn = filterFn;
    }

    public DistributedPredicate<? super T> filterFn() {
        return filterFn;
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, p.uniqueVertexName(name(), ""), localParallelism(), filterP(filterFn()));
        p.addEdges(this, pv.v);
    }
}
