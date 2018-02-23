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

import com.hazelcast.jet.function.DistributedObjLongBiFunction;
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
     * Default value of idle timeout to use
     */
    public static final long DEFAULT_IDLE_TIMEOUT = 60_000L;

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
    private final DistributedToLongFunction<? super T> timestampFn;
    private final DistributedSupplier<WatermarkPolicy> newWmPolicyFn;
    private final WatermarkEmissionPolicy wmEmitPolicy;
    private final DistributedObjLongBiFunction<? super T, ?> wrapFn;

    private WatermarkGenerationParams(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull DistributedSupplier<WatermarkPolicy> newWmPolicyFn,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            @Nonnull DistributedObjLongBiFunction<? super T, ?> wrapFn,
            long idleTimeoutMillis
    ) {
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.timestampFn = timestampFn;
        this.newWmPolicyFn = newWmPolicyFn;
        this.wmEmitPolicy = wmEmitPolicy;
        this.wrapFn = wrapFn;
    }

    /**
     * Creates new watermark generation parameters. See {@link #noWatermarks()}
     * if you don't need any watermarks.
     * @param timestampFn a function to extract timestamps from observed
     *      events.
     * @param wrapFn Javadoc pending
     * @param wmPolicy Javadoc pending
     * @param wmEmitPolicy watermark emission policy
     * @param idleTimeoutMillis a timeout after which the source partition will
     *      be marked as <em>idle</em>. If <=0, partitions will never be marked
     *      as idle.
     */
    public static <T> WatermarkGenerationParams<T> wmGenParams(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull DistributedObjLongBiFunction<? super T, ?> wrapFn,
            @Nonnull DistributedSupplier<WatermarkPolicy> wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        return new WatermarkGenerationParams<>(timestampFn, wmPolicy, wmEmitPolicy, wrapFn, idleTimeoutMillis);
    }

    /**
     * Creates new watermark generation parameters. See {@link #noWatermarks()}
     * if you don't need any watermarks.
     *
     * @param timestampFn a function to extract timestamps from observed
     *      events.
     * @param wmPolicy Javadoc pending
     * @param wmEmitPolicy watermark emission policy
     * @param idleTimeoutMillis a timeout after which the source partition will
     *      be marked as <em>idle</em>. If <=0, partitions will never be marked
     *      as idle.
     */
    public static <T> WatermarkGenerationParams<T> wmGenParams(
            @Nonnull DistributedToLongFunction<T> timestampFn,
            @Nonnull DistributedSupplier<WatermarkPolicy> wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        return wmGenParams(timestampFn, (t, l) -> t, wmPolicy, wmEmitPolicy, idleTimeoutMillis);
    }
    /**
     * Returns watermark generation parameters that will never emit any
     * watermark.
     * <p>
     * Only useful when using streaming sources in jobs where there is no
     * aggregation. For example to stream map journal events to a file etc.
     * If there is an aggregation in the job, you'll have no output at all with
     * this method.
     */
    public static <T> WatermarkGenerationParams<T> noWatermarks() {
        return wmGenParams(i -> Long.MIN_VALUE, NO_WATERMARK_POLICY, suppressDuplicates(), -1);
    }

    /**
     * A timeout after which the {@link
     * com.hazelcast.jet.impl.execution.WatermarkCoalescer#IDLE_MESSAGE} will
     * be sent. If <=0, partitions will never be marked as idle.
     */
    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    /**
     * Returns the function to extract timestamps from observed events.
     */
    @Nonnull
    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    /**
     * Returns watermark policy factory.
     */
    @Nonnull
    public DistributedSupplier<WatermarkPolicy> newWmPolicyFn() {
        return newWmPolicyFn;
    }

    /**
     * Returns watermark emission policy.
     */
    @Nonnull
    public WatermarkEmissionPolicy wmEmitPolicy() {
        return wmEmitPolicy;
    }

    /**
     * Javadoc pending.
     */
    @Nonnull
    public DistributedObjLongBiFunction<? super T, ?> wrapFn() {
        return wrapFn;
    }
}
