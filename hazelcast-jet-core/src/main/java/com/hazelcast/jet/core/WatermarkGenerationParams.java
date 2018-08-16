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

package com.hazelcast.jet.core;

import com.hazelcast.jet.function.DistributedObjLongBiFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;

import javax.annotation.Nonnull;
import java.io.Serializable;

import static com.hazelcast.jet.core.WatermarkEmissionPolicy.noThrottling;

/**
 * A holder of functions and parameters Jet needs to determine when and how
 * to insert watermarks into an event stream. These are the components:
 * <ul><li>
 *     {@code timestampFn}: extracts the timestamp from an event in the stream
 * </li><li>
 *     {@code newWmPolicyFn}: a factory of <em>watermark policy</em> objects.
 *     Refer to its {@link WatermarkPolicy documentation} for explanation.
 * </li><li>
 *     {@code wmEmitPolicy}: a {@link WatermarkEmissionPolicy policy object} that
 *     allows the processor to filter out redundant watermark items before
 *     emitting them. For example, a sliding/tumbling window processor doesn't
 *     need to observe more than one watermark item per frame.
 * </li><li>
 *     {@code idleTimeoutMillis}: a measure to mitigate the issue with temporary
 *     lulls in a distributed event stream. It pertains to each <em>partition
 *     </em> of a data source independently. If Jet doesn't receive any events
 *     from a given partition for this long, it will mark it as "idle" and let
 *     the watermark in downstream vertices advance as if the partition didn't
 *     exist.
 * </li><li>
 *     {@code wrapFn}: a function that transforms a given event and its
 *     timestamp into the item to emit from the processor. For example, the
 *     Pipeline API uses this to wrap items into {@code JetEvent}s as a way
 *     to propagate the event timestamps through the pipeline regardless of
 *     the transformation the user does on the event objects themselves.
 * </li></ul>
 *
 * @param <T> event type
 */
public final class WatermarkGenerationParams<T> implements Serializable {

    /**
     * The default idle timeout in milliseconds.
     */
    public static final long DEFAULT_IDLE_TIMEOUT = 60_000L;

    private static final DistributedSupplier<WatermarkPolicy> NO_WATERMARKS = () -> new WatermarkPolicy() {
        @Override
        public long reportEvent(long timestamp) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    };

    private final DistributedToLongFunction<? super T> timestampFn;
    private final DistributedObjLongBiFunction<? super T, ?> wrapFn;
    private final DistributedSupplier<? extends WatermarkPolicy> newWmPolicyFn;
    private final WatermarkEmissionPolicy wmEmitPolicy;
    private final long idleTimeoutMillis;

    private WatermarkGenerationParams(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull DistributedObjLongBiFunction<? super T, ?> wrapFn,
            @Nonnull DistributedSupplier<? extends WatermarkPolicy> newWmPolicyFn,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        this.timestampFn = timestampFn;
        this.newWmPolicyFn = newWmPolicyFn;
        this.wmEmitPolicy = wmEmitPolicy;
        this.wrapFn = wrapFn;
        this.idleTimeoutMillis = idleTimeoutMillis;
    }

    /**
     * Creates and returns new watermark generation parameters. To get
     * parameters that result in no watermarks being emitted, call {@link
     * #noWatermarks()}.
     *
     * @param timestampFn       function that extracts the timestamp from the event
     * @param wrapFn            function that transforms the received item and its timestamp into the
     *                          emitted item
     * @param newWmPolicyFn     factory of the watermark policy objects
     * @param wmEmitPolicy      watermark emission policy (decides how to suppress redundant watermarks)
     * @param idleTimeoutMillis the timeout after which a partition will be marked as <em>idle</em>.
     *                          If <= 0, partitions will never be marked as idle.
     */
    public static <T> WatermarkGenerationParams<T> wmGenParams(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull DistributedObjLongBiFunction<? super T, ?> wrapFn,
            @Nonnull DistributedSupplier<? extends WatermarkPolicy> newWmPolicyFn,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        return new WatermarkGenerationParams<>(timestampFn, wrapFn, newWmPolicyFn, wmEmitPolicy, idleTimeoutMillis);
    }

    /**
     * Creates and returns a watermark generation parameters object. To get
     * parameters that result in no watermarks being emitted, call {@link
     * #noWatermarks()}.
     *
     * @param timestampFn       function that extracts the timestamp from the event
     * @param wmPolicy          factory of the watermark policy objects
     * @param wmEmitPolicy      watermark emission policy (decides how to suppress redundant watermarks)
     * @param idleTimeoutMillis the timeout after which a partition will be marked as <em>idle</em>.
     *                          If <= 0, partitions will never be marked as idle.
     */
    public static <T> WatermarkGenerationParams<T> wmGenParams(
            @Nonnull DistributedToLongFunction<? super T> timestampFn,
            @Nonnull DistributedSupplier<? extends WatermarkPolicy> wmPolicy,
            @Nonnull WatermarkEmissionPolicy wmEmitPolicy,
            long idleTimeoutMillis
    ) {
        return wmGenParams(timestampFn, (event, timestamp) -> event, wmPolicy, wmEmitPolicy, idleTimeoutMillis);
    }

    /**
     * Returns watermark generation parameters that result in no watermarks
     * being emitted. Only useful in jobs with streaming sources that don't do
     * any aggregation. If there is an aggregation step in the job and you use
     * these parameters, your job will keep accumulating the data without
     * producing any output.
     */
    public static <T> WatermarkGenerationParams<T> noWatermarks() {
        return wmGenParams(i -> Long.MIN_VALUE, NO_WATERMARKS, noThrottling(), -1);
    }

    /**
     * Returns the function that extracts the timestamp from the event.
     */
    @Nonnull
    public DistributedToLongFunction<? super T> timestampFn() {
        return timestampFn;
    }

    /**
     * Returns the function that transforms the received item and its timestamp
     * into the emitted item.
     */
    @Nonnull
    public DistributedObjLongBiFunction<? super T, ?> wrapFn() {
        return wrapFn;
    }

    /**
     * Returns the factory of the watermark policy objects.
     */
    @Nonnull
    public DistributedSupplier<? extends WatermarkPolicy> newWmPolicyFn() {
        return newWmPolicyFn;
    }

    /**
     * Returns the watermark emission policy, which decides how to suppress
     * redundant watermarks.
     */
    @Nonnull
    public WatermarkEmissionPolicy wmEmitPolicy() {
        return wmEmitPolicy;
    }

    /**
     * Returns the amount of time allowed to pass without receiving any events
     * from a partition before marking it as "idle". When the partition
     * becomes idle, the processor emits an {@link
     * com.hazelcast.jet.impl.execution.WatermarkCoalescer#IDLE_MESSAGE} to its
     * output edges. This signals Jet that the watermark can advance as
     * if the partition didn't exist.
     * <p>
     * If supply a zero or negative value, partitions will never be marked as
     * idle.
     */
    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }

    /**
     * Returns new instance with emit policy replaced with the given argument.
     */
    @Nonnull
    public WatermarkGenerationParams<T> withEmitPolicy(WatermarkEmissionPolicy emitPolicy) {
        return wmGenParams(timestampFn, wrapFn, newWmPolicyFn, emitPolicy, idleTimeoutMillis);
    }
}
