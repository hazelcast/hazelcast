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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.pipeline.PipelineImpl;

import javax.annotation.Nonnull;
import java.io.Serializable;

/**
 * Models a distributed computation job using an analogy with a system of
 * interconnected water pipes. The basic element is a <em>stage</em> which
 * can be attached to one or more other stages. A stage accepts the data
 * coming from its upstream stages, transforms it, and directs the
 * resulting data to its downstream stages.
 * <p>
 * The {@code Pipeline} object is a container of all the stages defined on
 * a pipeline: the source stages obtained directly from it by calling {@link
 * #readFrom(BatchSource)} as well as all the stages attached (directly or
 * indirectly) to them.
 * <p>
 * Note that there is no simple one-to-one correspondence between pipeline
 * stages and Core API's DAG vertices. Some stages map to several vertices
 * (e.g., grouping and co-grouping are implemented as a cascade of two
 * vertices) and some stages may be merged with others into a single vertex
 * (e.g., a cascade of map/filter/flatMap stages can be fused into one
 * vertex).
 *
 * @since Jet 3.0
 */
public interface Pipeline extends Serializable {

    /**
     * Creates a new, empty pipeline.
     *
     * @since Jet 3.0
     */
    @Nonnull
    static Pipeline create() {
        return new PipelineImpl();
    }

    /**
     * Returns the preserve order property of this pipeline
     * @since Jet 4.4
     */
    boolean isPreserveOrder();

    /**
     * Tells Jet whether or not it is allowed to reorder the events for better
     * performance. Enabling this property adds restrictions to the internal
     * data flows that ensure each pipeline stage observes the events in the
     * same order as they were received from the source.
     * <p>
     * Keep in mind that there is often no total event order to begin with,
     * for example in partitioned sources. In this case there is still partial
     * order per each partition, and that is the order Jet preserves.
     * <p>
     * If you enable this property, you cannot use the {@link
     * GeneralStage#rebalance() rebalance} operator without a grouping key,
     * because it explicitly orders Jet to break the event order.
     * <p>
     * The default value is false.
     *
     * @return {@code this}, for fluent API
     * @since Jet 4.4
     */
    @Nonnull
    Pipeline setPreserveOrder(boolean value);

    /**
     * Returns a pipeline stage that represents a bounded (batch) data source. It
     * has no upstream stages and emits the data (typically coming from an outside
     * source) to its downstream stages.
     *
     * @param source the definition of the source from which the stage reads data
     * @param <T> the type of source data items
     */
    @Nonnull
    <T> BatchStage<T> readFrom(@Nonnull BatchSource<? extends T> source);

    /**
     * Returns a pipeline stage that represents an unbounded data source (i.e., an
     * event stream). It has no upstream stages and emits the data (typically coming
     * from an outside source) to its downstream stages.
     *
     * @param source the definition of the source from which the stage reads data
     * @param <T> the type of source data items
     */
    @Nonnull
    <T> StreamSourceStage<T> readFrom(@Nonnull StreamSource<? extends T> source);

    /**
     * Attaches the supplied sink to two or more pipeline stages. Returns the
     * {@code SinkStage} representing the sink. You need this method when you
     * want to drain more than one stage to the same sink. In the typical case
     * you'll use {@link GeneralStage#writeTo(Sink)} instead.
     *
     * @param <T> the type of data being drained to the sink
     */
    @Nonnull
    <T> SinkStage writeTo(
            @Nonnull Sink<? super T> sink,
            @Nonnull GeneralStage<? extends T> stage0,
            @Nonnull GeneralStage<? extends T> stage1,
            @Nonnull GeneralStage<? extends T>... moreStages
    );

    /**
     * Transforms the pipeline into a Jet DAG, which can be submitted for
     * execution to a Jet instance.
     * @deprecated since Jet 4.3, Jet performs this transformation on the server-side.
     */
    @Nonnull
    @Deprecated()
    DAG toDag();

    /**
     * Returns a DOT format (graphviz) representation of the Pipeline.
     */
    @Nonnull
    String toDotString();

    /**
     * Returns {@code true} if there are no stages in the pipeline.
     *
     * @since Jet 4.4
     */
    boolean isEmpty();
}
