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

import com.hazelcast.jet.core.Vertex;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public abstract class AbstractTransform implements Transform {
    @Nonnull
    private String name;
    @Nonnull
    private final List<Transform> upstream;
    @Nonnull
    private Optimization optimization = Optimization.NETWORK_TRAFFIC;

    private int localParallelism = Vertex.LOCAL_PARALLELISM_USE_DEFAULT;

    protected AbstractTransform(@Nonnull String name, @Nonnull List<Transform> upstream) {
        this.name = name;
        this.upstream = upstream;
    }

    protected AbstractTransform(String name, @Nonnull Transform upstream) {
        this(name, new ArrayList<>(singletonList(upstream)));
    }

    @Nonnull @Override
    public List<Transform> upstream() {
        return upstream;
    }

    @Override
    public void setName(@Nonnull String name) {
        this.name = name;
    }

    @Nonnull @Override
    public String name() {
        return name;
    }

    @Override
    public void localParallelism(int localParallelism) {
        this.localParallelism = Vertex.checkLocalParallelism(localParallelism);
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    @Nonnull
    Optimization getOptimization() {
        return optimization;
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public long preferredWatermarkStride() {
        return 0;
    }

    public enum Optimization {
        NETWORK_TRAFFIC,
        MEMORY
    }
}
