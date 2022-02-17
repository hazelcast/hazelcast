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

import com.hazelcast.function.FunctionEx;
import com.hazelcast.function.PredicateEx;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.peekOutputP;

public class PeekTransform<T> extends AbstractTransform {

    private static final long serialVersionUID = 1L;

    @Nonnull
    public final PredicateEx<? super T> shouldLogFn;
    @Nonnull
    public final FunctionEx<? super T, ? extends CharSequence> toStringFn;

    public PeekTransform(
            @Nonnull Transform upstream,
            @Nonnull PredicateEx<? super T> shouldLogFn,
            @Nonnull FunctionEx<? super T, ? extends CharSequence> toStringFn
    ) {
        super("peek", upstream);
        this.shouldLogFn = shouldLogFn;
        this.toStringFn = toStringFn;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        determineLocalParallelism(LOCAL_PARALLELISM_USE_DEFAULT, context, p.isPreserveOrder());
        PlannerVertex peekedPv = p.xform2vertex.get(this.upstream().get(0));
        // Peeking transform doesn't add a vertex, so point to the upstream
        // transform's vertex:
        p.xform2vertex.put(this, peekedPv);
        peekedPv.v.updateMetaSupplier(sup -> peekOutputP(toStringFn, shouldLogFn, sup));
    }
}
