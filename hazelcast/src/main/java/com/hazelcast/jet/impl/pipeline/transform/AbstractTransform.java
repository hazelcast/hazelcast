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
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.hazelcast.jet.core.Vertex.LOCAL_PARALLELISM_USE_DEFAULT;
import static com.hazelcast.jet.core.Vertex.checkLocalParallelism;
import static java.lang.Math.min;
import static java.util.Collections.singletonList;

public abstract class AbstractTransform implements Transform {

    @Nonnull
    private String name;

    @Nonnull
    private List<Transform> upstream;

    private int localParallelism = LOCAL_PARALLELISM_USE_DEFAULT;

    private int determinedLocalParallelism = LOCAL_PARALLELISM_USE_DEFAULT;

    private boolean[] upstreamRebalancingFlags;

    private FunctionEx<?, ?>[] upstreamPartitionKeyFns;

    public AbstractTransform() {
    }

    protected AbstractTransform(@Nonnull String name, @Nonnull List<Transform> upstream) {
        this.name = name;
        // Planner updates this list to fuse the stateless transforms:
        this.upstream = new ArrayList<>(upstream);
        this.upstreamRebalancingFlags = new boolean[upstream.size()];
        this.upstreamPartitionKeyFns = new FunctionEx[upstream.size()];
    }

    protected AbstractTransform(String name, @Nonnull Transform upstream) {
        this(name, singletonList(upstream));
    }

    @Nonnull @Override
    public List<Transform> upstream() {
        return upstream;
    }

    void setUpstream(@Nonnull List<Transform> upstream) {
        this.upstream = upstream;
    }

    @Override
    public void setName(@Nonnull String name) {
        this.name = Objects.requireNonNull(name, "name");
    }

    @Nonnull @Override
    public String name() {
        return name;
    }

    @Override
    public void localParallelism(int localParallelism) {
        this.localParallelism = checkLocalParallelism(localParallelism);
    }

    @Override
    public int localParallelism() {
        return localParallelism;
    }

    @Override
    public void determinedLocalParallelism(int determinedLocalParallelism) {
        this.determinedLocalParallelism = checkLocalParallelism(determinedLocalParallelism);
    }

    @Override
    public int determinedLocalParallelism() {
        return determinedLocalParallelism;
    }

    @Override
    public void setRebalanceInput(int ordinal, boolean value) {
        upstreamRebalancingFlags[ordinal] = value;
    }

    @Override
    public boolean shouldRebalanceInput(int ordinal) {
        return upstreamRebalancingFlags[ordinal];
    }

    protected boolean[] upstreamRebalancingFlags() {
        return upstreamRebalancingFlags;
    }

    @Override
    public void setPartitionKeyFnForInput(int ordinal, FunctionEx<?, ?> keyFn) {
        upstreamPartitionKeyFns[ordinal] = keyFn;
    }

    protected FunctionEx<?, ?>[] upstreamPartitionKeyFns() {
        return upstreamPartitionKeyFns;
    }

    @Override
    public FunctionEx<?, ?> partitionKeyFnForInput(int ordinal) {
        return upstreamPartitionKeyFns[ordinal];
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public long preferredWatermarkStride() {
        return 0;
    }

    protected final boolean shouldRebalanceAnyInput() {
        for (boolean b : upstreamRebalancingFlags) {
            if (b) {
                return true;
            }
        }
        return false;
    }


    /**
     * Determines the local parallelism of the transform by looking at
     * its preferred local parallelism, the local parallelism of its upstream
     * transforms, and the default local parallelism in {@code
     * PipelineImpl.Context}.
     * <p>
     * If none of them is set, returns the default local parallelism provided
     * {@code PipelineImpl.Context}.
     */
    protected void determineLocalParallelism(
            int preferredLocalParallelism,
            Context context,
            boolean shouldMatchUpstreamParallelism
    ) {
        int defaultParallelism = context.defaultLocalParallelism();
        int upstreamParallelism = LOCAL_PARALLELISM_USE_DEFAULT;

        if (shouldMatchUpstreamParallelism) {
            // Get the minimum of upstream LPs as upstreamParallelism
            if (!upstream().isEmpty()) {
                upstreamParallelism = upstream()
                        .stream()
                        .mapToInt(Transform::determinedLocalParallelism)
                        .min()
                        .getAsInt();
            }
        }

        int currParallelism;
        if (localParallelism() == LOCAL_PARALLELISM_USE_DEFAULT) {
            if (preferredLocalParallelism == LOCAL_PARALLELISM_USE_DEFAULT) {
                currParallelism = defaultParallelism;
            } else {
                if (defaultParallelism == LOCAL_PARALLELISM_USE_DEFAULT) {
                    currParallelism = preferredLocalParallelism;
                } else {
                    currParallelism = min(preferredLocalParallelism, defaultParallelism);
                }
            }
        } else {
            currParallelism = localParallelism();
        }

        if (upstreamParallelism != LOCAL_PARALLELISM_USE_DEFAULT) {
            if (currParallelism != LOCAL_PARALLELISM_USE_DEFAULT) {
                determinedLocalParallelism(min(upstreamParallelism, currParallelism));
            } else {
                determinedLocalParallelism(upstreamParallelism);
            }
        } else {
            determinedLocalParallelism(currParallelism);
        }
    }
}
