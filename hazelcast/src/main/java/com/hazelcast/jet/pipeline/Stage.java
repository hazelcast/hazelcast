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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.Vertex;

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
 * To build a pipeline, start with {@link Pipeline#readFrom pipeline.readFrom()} to
 * get the initial stage and then use its methods to attach further
 * downstream stages. Terminate the pipeline by calling {@link
 * GeneralStage#writeTo(Sink) stage.writeTo(sink)}, which will attach a
 * {@link SinkStage}.
 *
 * @since Jet 3.0
 */
public interface Stage {
    /**
     * Returns the {@link Pipeline} this stage belongs to.
     */
    Pipeline getPipeline();

    /**
     * Sets the preferred local parallelism (number of processors per Jet
     * cluster member) this stage will configure its DAG vertices with. Jet
     * always uses the same number of processors on each member, so the total
     * parallelism automatically increases if another member joins the cluster.
     * <p>
     * While most stages are backed by 1 vertex, there are exceptions. If a
     * stage uses two vertices, each of them will have the given local
     * parallelism, so in total there will be twice as many processors per
     * member.
     * <p>
     * The default value is {@value
     * Vertex#LOCAL_PARALLELISM_USE_DEFAULT} and it signals
     * to Jet to figure out a default value. Jet will determine the vertex's local
     * parallelism during job initialization from the global default and the
     * processor meta-supplier's preferred value.
     *
     * @return this stage
     */
    @Nonnull
    Stage setLocalParallelism(int localParallelism);

    /**
     * Overrides the default name of the stage with the name you choose and
     * returns the stage. This can be useful for debugging purposes, to better
     * distinguish pipeline stages in the diagnostic output.
     *
     * @param name the stage name
     * @return this stage
     */
    @Nonnull
    Stage setName(@Nonnull String name);

    /**
     * Returns the name of this stage. It's used in diagnostic output.
     */
    @Nonnull
    String name();
}
