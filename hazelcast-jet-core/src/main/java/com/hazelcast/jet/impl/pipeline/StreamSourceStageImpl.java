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

package com.hazelcast.jet.impl.pipeline;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.impl.pipeline.transform.StreamSourceTransform;
import com.hazelcast.jet.pipeline.StreamSourceStage;
import com.hazelcast.jet.pipeline.StreamStage;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.ADAPT_TO_JET_EVENT;
import static com.hazelcast.jet.impl.pipeline.ComputeStageImplBase.DO_NOT_ADAPT;

public class StreamSourceStageImpl<T> implements StreamSourceStage<T> {

    private final StreamSourceTransform<T> transform;
    private final PipelineImpl pipeline;

    StreamSourceStageImpl(StreamSourceTransform<T> transform, PipelineImpl pipeline) {
        this.transform = transform;
        this.pipeline = pipeline;
    }

    @Override
    public StreamStage<T> withNativeTimestamps(long allowedLag) {
        if (!transform.supportsNativeTimestamps()) {
            throw new JetException("The source doesn't support native timestamps");
        }
        StreamStageImpl<T> result = createStreamStage();
        result.addTimestampsInt(null, allowedLag, true);
        return result;
    }

    @Override
    public StreamStage<T> withTimestamps(@Nonnull DistributedToLongFunction<? super T> timestampFn, long allowedLag) {
        StreamStageImpl<T> result = createStreamStage();
        result.addTimestampsInt(timestampFn, allowedLag, true);
        return result;
    }

    @Override
    public StreamStage<T> withoutTimestamps() {
        return createStreamStage();
    }

    private StreamStageImpl<T> createStreamStage() {
        return new StreamStageImpl<>(
                transform,
                transform.emitsJetEvents() ? ADAPT_TO_JET_EVENT : DO_NOT_ADAPT,
                pipeline);
    }
}
