/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.function.FunctionEx;
import com.hazelcast.jet.function.ToLongFunctionEx;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

import static com.hazelcast.jet.core.processor.Processors.mapStatefulP;
import static java.lang.Math.max;

public class MapStatefulTransform<T, K, S, R> extends AbstractTransform {

    private static final int TTL_TO_WM_STRIDE_RATIO = 4;
    private final long ttl;
    private final FunctionEx<? super T, ? extends K> keyFn;
    private final ToLongFunctionEx<? super T> timestampFn;
    private final Supplier<? extends S> createFn;
    private final TriFunction<? super S, ? super K, ? super T, ? extends R> statefulMapFn;
    @Nullable private TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn;

    public MapStatefulTransform(
            @Nonnull Transform upstream,
            long ttl,
            @Nonnull FunctionEx<? super T, ? extends K> keyFn,
            @Nonnull ToLongFunctionEx<? super T> timestampFn,
            @Nonnull Supplier<? extends S> createFn,
            @Nonnull TriFunction<? super S, ? super K, ? super T, ? extends R> statefulMapFn,
            @Nullable TriFunction<? super S, ? super K, ? super Long, ? extends R> onEvictFn
    ) {
        super("transform-stateful", upstream);
        this.ttl = ttl;
        this.keyFn = keyFn;
        this.timestampFn = timestampFn;
        this.createFn = createFn;
        this.statefulMapFn = statefulMapFn;
        this.onEvictFn = onEvictFn;
    }

    @Override
    public long preferredWatermarkStride() {
        return max(1, ttl / TTL_TO_WM_STRIDE_RATIO);
    }

    @Override
    public void addToDag(Planner p) {
        PlannerVertex pv = p.addVertex(this, name(), localParallelism(),
                mapStatefulP(ttl, keyFn, timestampFn, createFn, statefulMapFn, onEvictFn));
        p.addEdges(this, pv.v, edge -> edge.partitioned(keyFn).distributed());
    }
}
