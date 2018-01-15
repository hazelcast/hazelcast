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

package com.hazelcast.jet.core;

import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;

/**
 * Wrapper for functions and parameters needed to generate Watermarks.
 *
 * @param <T> event type
 */
public final class WatermarkGenerationParams<T> implements Serializable {

    /**
     * A watermark policy that will never emit any watermark.
     */
    private static final DistributedSupplier<WatermarkPolicy> NO_WATERMARK_POLICY = () -> new WatermarkPolicy() {
        @Override
        public long reportEvent(long timestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    };

    private final long idleTimeoutMillis;
    @Nonnull private final DistributedToLongFunction<T> getTimestampF;
    @Nonnull private final DistributedSupplier<WatermarkPolicy> newWmPolicyF;
    @Nonnull private final WatermarkEmissionPolicy wmEmitPolicy;

    private WatermarkGenerationParams(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.getTimestampF = getTimestampF;
        this.newWmPolicyF = newWmPolicyF;
        this.wmEmitPolicy = wmEmitPolicy;
    }

    /**
     * Creates new watermark generation parameters. See {@link #noWatermarks()}
     * if you don't need any watermarks.
     *
     * @param getTimestampF a function to extract timestamps from observed
     *      events.
     * @param newWmPolicyF watermark policy factory
     * @param wmEmitPolicy watermark emission policy
     * @param idleTimeoutMillis a timeout after which the {@link
     *      com.hazelcast.jet.impl.execution.WatermarkCoalescer#IDLE_MESSAGE}
     *      will be sent.
     */
    public static <T> WatermarkGenerationParams<T> wmGenParams(
            @Nonnull DistributedToLongFunction<T> getTimestampF,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyF,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        return new WatermarkGenerationParams<>(getTimestampF, newWmPolicyF, wmEmitPolicy, idleTimeoutMillis);
    }

    /**
     * Returns watermark generation parameters that will never emit any
     * watermark.
     */
    public static <T> WatermarkGenerationParams<T> noWatermarks() {
        return new WatermarkGenerationParams<>(i -> Long.MIN_VALUE, NO_WATERMARK_POLICY, suppressDuplicates(), -1);
    }

    /**
     * A timeout after which the {@link
     * com.hazelcast.jet.impl.execution.WatermarkCoalescer#IDLE_MESSAGE} will
     * be sent.
     */
    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    /**
     * Returns the function to extract timestamps from observed events.
     */
    @Nonnull
    public DistributedToLongFunction<T> getTimestampF() {
        return getTimestampF;
    }

    /**
     * Returns watermark policy factory.
     */
    @Nonnull
    public DistributedSupplier<WatermarkPolicy> newWmPolicyF() {
        return newWmPolicyF;
    }

    /**
     * Returns watermark emission policy.
     */
    @Nonnull
    public WatermarkEmissionPolicy wmEmitPolicy() {
        return wmEmitPolicy;
    }
}
