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

import com.hazelcast.jet.impl.pipeline.Planner;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * This is a pure data object and holds no implementation code for the
 * transformation it represents. {@link Planner} is the implementation class
 * that creates a Core API DAG for a pipeline.
 */
public interface Transform {
    /**
     * Returns the name of this transformation.
     */
    @Nonnull
    String name();

    /**
     * Sets a descriptive name for this transform.
     *
     * @param name the stage name. If {@code null}, the stage will use its default name
     */
    void setName(@Nonnull String name);

    int localParallelism();

    void localParallelism(int localParallelism);

    @Nonnull
    List<Transform> upstream();

    void addToDag(Planner p);

    /**
     * Returns the frame length for which this transform needs watermarks. If
     * {@code frameLength == 0}, this transform doesn't need watermarks.
     */
    long watermarkFrameSize();
}
