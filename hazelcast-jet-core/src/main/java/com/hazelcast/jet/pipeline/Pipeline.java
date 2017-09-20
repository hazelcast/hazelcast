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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.impl.PipelineImpl;

import javax.annotation.Nonnull;
import java.util.concurrent.Future;

/**
 * Models a distributed computation job using an analogy with a system of
 * interconnected water pipes. The basic element is a <em>stage</em> which
 * can be attached to one or more other stages. A stage accepts the data
 * coming from its upstream stages, transforms it, and directs the
 * resulting data to its downstream stages.
 * <p>
 * The {@code Pipeline} object is a container of all the stages defined on
 * a pipeline: the source stages obtained directly from it by calling {@link
 * #drawFrom(Source)} as well as all the stages attached (directly or
 * indirectly) to them.
 * <p>
 * Note that there is no simple one-to-one correspondence between pipeline
 * stages and Core API's DAG vertices. Some stages map to several vertices
 * (e.g., grouping and co-grouping are implemented as a cascade of two
 * vertices) and some stages may be merged with others into a single vertex
 * (e.g., a cascade of map/filter/flatMap stages can be fused into one
 * vertex).
 */
public interface Pipeline {

    /**
     * Returns a new pipeline stage that has no upstream stages and produces
     * some output for its downstream stages.
     *
     * @param source the definition of the source from which the stage draws data
     * @param <E> the type of source data items
     */
    @Nonnull
    <E> ComputeStage<E> drawFrom(@Nonnull Source<E> source);

    /**
     * Transforms the pipeline into a Jet job and executes it.
     *
     * @param jet the Jet instance that will execute the job
     * @return the job's {@link com.hazelcast.jet.Job#getFuture() future} object
     */
    @Nonnull
    default Future<Void> execute(@Nonnull JetInstance jet) {
        return jet.newJob(toDag()).getFuture();
    }

    /**
     * Transforms the pipeline into a Jet DAG, which can be submitted for
     * execution to a Jet instance.
     */
    @Nonnull
    DAG toDag();

    /**
     * Creates a new, empty pipeline.
     */
    @Nonnull
    static Pipeline create() {
        return new PipelineImpl();
    }
}
