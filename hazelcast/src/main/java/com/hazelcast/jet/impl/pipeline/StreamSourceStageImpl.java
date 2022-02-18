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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.function.ObjLongBiFunction;
import com.hazelcast.jet.impl.JetEvent;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.WatermarkPolicy.limitingRealTimeLag;
import static com.hazelcast.jet.impl.JetEvent.jetEvent;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DO_NOT_ADAPT;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class StreamSourceStageImpl<T> implements StreamSourceStage<T> {

    private static final ObjLongBiFunction WRAP_TO_JET_EVENT = (item, ts) -> jetEvent(ts, item);
    private final StreamSourceTransform<T> transform;
    private final PipelineImpl pipeline;

    StreamSourceStageImpl(StreamSourceTransform<T> transform, PipelineImpl pipeline) {
        this.transform = transform;
        this.pipeline = pipeline;
    }

    @Override
    public StreamStage<T> withIngestionTimestamps() {
        transform.setEventTimePolicy(eventTimePolicy(
                o -> System.currentTimeMillis(),
                wrapToJetEvent(),
                limitingRealTimeLag(0),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, ADAPT_TO_JET_EVENT, pipeline);
    }

    @Override
    public StreamStage<T> withNativeTimestamps(long allowedLag) {
        checkTrue(transform.supportsNativeTimestamps(), "The source doesn't support native timestamps");
        transform.setEventTimePolicy(eventTimePolicy(
                null,
                wrapToJetEvent(),
                limitingLag(allowedLag),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, ADAPT_TO_JET_EVENT, pipeline);
    }

    @Override
    public StreamStage<T> withTimestamps(@Nonnull ToLongFunctionEx<? super T> timestampFn, long allowedLag) {
        checkSerializable(timestampFn, "timestampFn");
        transform.setEventTimePolicy(eventTimePolicy(
                timestampFn,
                wrapToJetEvent(),
                limitingLag(allowedLag),
                0,
                0,
                transform.partitionIdleTimeout()
        ));
        return new StreamStageImpl<>(transform, ADAPT_TO_JET_EVENT, pipeline);
    }

    @Override
    public StreamStage<T> withoutTimestamps() {
        return new StreamStageImpl<>(
                transform,
                transform.emitsJetEvents() ? ADAPT_TO_JET_EVENT : DO_NOT_ADAPT,
                pipeline);
    }

    @SuppressWarnings("unchecked")
    private ObjLongBiFunction<T, JetEvent<T>> wrapToJetEvent() {
        return WRAP_TO_JET_EVENT;
    }
}
