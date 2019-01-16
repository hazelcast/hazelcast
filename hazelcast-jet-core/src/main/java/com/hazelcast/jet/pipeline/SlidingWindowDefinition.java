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

package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.core.SlidingWindowPolicy;

import javax.annotation.Nonnull;

import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.pipeline.WindowDefinition.WindowKind.SLIDING;
import static com.hazelcast.util.Preconditions.checkPositive;
import static com.hazelcast.util.Preconditions.checkTrue;

/**
 * Represents the definition of a {@link WindowKind#SLIDING sliding
 * window}.
 */
public class SlidingWindowDefinition extends WindowDefinitionBase {
    private final long windowSize;
    private final long slideBy;

    SlidingWindowDefinition(long windowSize, long slideBy) {
        checkPositive(windowSize, "windowSize must be positive");
        checkPositive(slideBy, "slideBy must be positive");
        checkTrue(windowSize % slideBy == 0, "windowSize must be integer multiple of slideBy, mod("
                + windowSize + ", " + slideBy + ") != 0");
        this.windowSize = windowSize;
        this.slideBy = slideBy;
    }

    @Nonnull @Override
    public WindowKind kind() {
        return SLIDING;
    }

    @Nonnull @Override
    @SuppressWarnings("unchecked")
    public SlidingWindowDefinition downcast() {
        return this;
    }

    @Override
    public long preferredWatermarkStride() {
        return slideBy;
    }

    /**
     * Returns the length of the window (the size of the timestamp range it
     * covers). It is an integer multiple of {@link #slideBy()}.
     */
    public long windowSize() {
        return windowSize;
    }

    /**
     * Returns the size of the sliding step.
     */
    public long slideBy() {
        return slideBy;
    }

    /**
     * Returns the policy object used by the Core API processors to drive the
     * sliding window logic.
     * */
    public SlidingWindowPolicy toSlidingWindowPolicy() {
        return slidingWinPolicy(windowSize, slideBy);
    }
}
