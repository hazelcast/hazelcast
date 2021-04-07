/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.core.EventTimePolicy;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.impl.pipeline.Planner;
import com.hazelcast.jet.impl.pipeline.Planner.PlannerVertex;
import com.hazelcast.jet.impl.pipeline.PipelineImpl.Context;
import com.hazelcast.jet.pipeline.StreamSource;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.DEFAULT_IDLE_TIMEOUT;
import static com.hazelcast.jet.core.EventTimePolicy.noEventTime;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static java.util.Collections.emptyList;

public class StreamSourceTransform<T> extends AbstractTransform implements StreamSource<T> {

    public final FunctionEx<? super EventTimePolicy<? super T>, ? extends ProcessorMetaSupplier> metaSupplierFn;
    private boolean isAssignedToStage;
    private final boolean emitsWatermarks;

    @Nullable
    private EventTimePolicy<? super T> eventTimePolicy;
    private final boolean supportsNativeTimestamps;
    private long partitionIdleTimeout = DEFAULT_IDLE_TIMEOUT;

    public StreamSourceTransform(
            @Nonnull String name,
            @Nonnull FunctionEx<? super EventTimePolicy<? super T>, ? extends ProcessorMetaSupplier> metaSupplierFn,
            boolean emitsWatermarks,
            boolean supportsNativeTimestamps
    ) {
        super(name, emptyList());
        this.metaSupplierFn = metaSupplierFn;
        this.emitsWatermarks = emitsWatermarks;
        this.supportsNativeTimestamps = supportsNativeTimestamps;
    }

    public void onAssignToStage() {
        if (isAssignedToStage) {
            throw new IllegalStateException("Source " + name() + " was already assigned to a source stage");
        }
        isAssignedToStage = true;
    }

    @Override
    public void addToDag(Planner p, Context context) {
        if (emitsWatermarks || eventTimePolicy == null) {
            // Reached when the source either emits both JetEvents and watermarks
            // or neither. In these cases we don't have to insert watermarks.
            final ProcessorMetaSupplier metaSupplier =
                    metaSupplierFn.apply(eventTimePolicy != null ? eventTimePolicy : noEventTime());
            determineLocalParallelism(metaSupplier.preferredLocalParallelism(), context, false);
            p.addVertex(this, name(), determinedLocalParallelism(), metaSupplier);
        } else {
            //                  ------------
            //                 |  sourceP   |
            //                  ------------
            //                       |
            //                    isolated
            //                       v
            //                  -------------
            //                 |  insertWmP  |
            //                  -------------
            String v1name = name();
            final ProcessorMetaSupplier metaSupplier = metaSupplierFn.apply(eventTimePolicy);
            determineLocalParallelism(metaSupplier.preferredLocalParallelism(), context, false);
            Vertex v1 = p.dag.newVertex(v1name, metaSupplier)
                             .localParallelism(determinedLocalParallelism());
            PlannerVertex pv2 = p.addVertex(
                    this, v1name + "-add-timestamps", determinedLocalParallelism(), insertWatermarksP(eventTimePolicy)
            );
            p.dag.edge(between(v1, pv2.v).isolated());
        }
    }

    @Nullable
    public EventTimePolicy<? super T> getEventTimePolicy() {
        return eventTimePolicy;
    }

    public void setEventTimePolicy(@Nonnull EventTimePolicy<? super T> eventTimePolicy) {
        this.eventTimePolicy = eventTimePolicy;
    }

    public boolean emitsJetEvents() {
        return eventTimePolicy != null;
    }

    @Override
    public boolean supportsNativeTimestamps() {
        return supportsNativeTimestamps;
    }

    @Override
    public StreamSource<T> setPartitionIdleTimeout(long timeoutMillis) {
        checkNotNegative(timeoutMillis, "timeout must be >= 0 (0 means disabled)");
        this.partitionIdleTimeout = timeoutMillis;
        return this;
    }

    @Override
    public long partitionIdleTimeout() {
        return partitionIdleTimeout;
    }
}
