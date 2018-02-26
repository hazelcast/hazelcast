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

import com.hazelcast.jet.core.SlidingWindowPolicy;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.SlidingWindowPolicy.tumblingWinPolicy;
import static com.hazelcast.jet.pipeline.WindowDefinition.WindowKind.TUMBLING;

/**
 * Represents the definition of a {@link WindowKind#TUMBLING tumbling
 * window}.
 */
public class TumblingWindowDef extends SlidingWindowDef {

    TumblingWindowDef(long windowSize) {
        super(windowSize, windowSize);
    }

    @Nonnull @Override
    public WindowKind kind() {
        return TUMBLING;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public TumblingWindowDef downcast() {
        return this;
    }

    /**
     * Returns the length of the window (the size of the timestamp range it
     * covers).
     */
    @Override
    public long windowSize() {
        return super.windowSize();
    }

    /**
     * A tumbling window advances in steps equal to its size, therefore this
     * method always returns the same value as {@link #windowSize()}.
     */
    public long slideBy() {
        return super.slideBy();
    }

    @Override
    public SlidingWindowPolicy toSlidingWindowPolicy() {
        return tumblingWinPolicy(windowSize());
    }
}
