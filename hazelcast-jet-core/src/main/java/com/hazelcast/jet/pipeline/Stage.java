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

package com.hazelcast.jet.pipeline;

import javax.annotation.Nonnull;

/**
 * The basic element of a Jet {@link Pipeline pipeline}, represents
 * a computation step. It accepts input from its upstream stages (if any)
 * and passes its output to its downstream stages (if any). Jet
 * differentiates between {@link BatchStage batch stages} that represent
 * finite data sets (batches) and {@link StreamStage stream stages} that
 * represent infinite data streams. Some operations only make sense on a
 * batch stage and vice versa.
 * <p>
 * To build a pipeline, start with {@link Pipeline#drawFrom pipeline.drawfrom()} to
 * get the initial stage and then use its methods to attach further
 * downstream stages. Terminate the pipeline by calling {@link
 * GeneralStage#drainTo(Sink) stage.drainTo(sink)}, which will attach a
 * {@link SinkStage}.
 */
public interface Stage {
    /**
     * Returns the {@link Pipeline} this stage belongs to.
     */
    Pipeline getPipeline();

    /**
     * Sets the number of processors running DAG vertices backing this stage
     * that will be created on each member. Most stages are backed by 1 vertex,
     * some are backed by multiple vertices or no vertex at all.
     * <p>
     * The value specifies <em>local</em> parallelism, i.e. the number of
     * parallel workers running on each member. Total (global) parallelism is
     * determined as <em>localParallelism * numberOfMembers</em>. If a new
     * member is added, the total parallelism is increased.
     * <p>
     * If the value is {@value
     * com.hazelcast.jet.core.Vertex#LOCAL_PARALLELISM_USE_DEFAULT}, Jet will
     * determine the vertex's local parallelism during job initialization
     * from the global default and processor meta-supplier's preferred value.
     */
    @Nonnull
    Stage setLocalParallelism(int localParallelism);

    /**
     * Overrides the default name of the stage with the name you choose. This
     * can be useful for debugging purposes, to better distinguish pipeline
     * stages in the diagnostic output.
     *
     * @param name the stage name
     */
    @Nonnull
    Stage setName(@Nonnull String name);

    /**
     * Returns the display name of this stage. It's used in diagnostic output.
     */
    @Nonnull
    String name();
}
