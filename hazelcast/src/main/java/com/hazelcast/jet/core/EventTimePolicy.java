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

package com.hazelcast.jet.core;

import com.hazelcast.function.SupplierEx;
import com.hazelcast.function.ToLongFunctionEx;
import com.hazelcast.jet.core.function.ObjLongBiFunction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;

/**
 * A holder of functions and parameters Jet needs to handle event time and the
 * associated watermarks. These are the components:
 * <ul><li>
 *     {@code timestampFn}: extracts the timestamp from an event in the stream
 * </li><li>
 *     {@code newWmPolicyFn}: a factory of <em>watermark policy</em> objects.
 *     Refer to its {@linkplain WatermarkPolicy documentation} for explanation.
 * </li><li>
 *     <i>frame size</i> and <i>frame offset</i> for <i>watermark throttling</i>:
 *     they allow the processor to filter out redundant watermark items before
 *     emitting them. For example, a sliding/tumbling window processor doesn't need
 *     to observe more than one watermark item per frame.
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
 * This class should be used with {@link EventTimeMapper} when implementing a
 * source processor.
 *
 * @param <T> event type
 *
 * @since Jet 3.0
 */
public final class EventTimePolicy<T> implements Serializable {

    /**
     * The default idle timeout in milliseconds.
     */
    public static final long DEFAULT_IDLE_TIMEOUT = 60_000L;

    private static final long serialVersionUID = 1L;

    private static final ObjLongBiFunction<?, ?> NO_WRAPPING = (event, timestamp) -> event;

    private static final SupplierEx<WatermarkPolicy> NO_WATERMARKS = () -> new WatermarkPolicy() {
        @Override
        public void reportEvent(long timestamp) {
        }

        @Override
        public long getCurrentWatermark() {
            return Long.MIN_VALUE;
        }
    };

    private final ToLongFunctionEx<? super T> timestampFn;
    private final ObjLongBiFunction<? super T, ?> wrapFn;
    private final SupplierEx<? extends WatermarkPolicy> newWmPolicyFn;
    private final long watermarkThrottlingFrameSize;
    private final long watermarkThrottlingFrameOffset;
    private final long idleTimeoutMillis;

    private EventTimePolicy(
            @Nullable ToLongFunctionEx<? super T> timestampFn,
            @Nonnull ObjLongBiFunction<? super T, ?> wrapFn,
            @Nonnull SupplierEx<? extends WatermarkPolicy> newWmPolicyFn,
            long watermarkThrottlingFrameSize,
            long watermarkThrottlingFrameOffset,
            long idleTimeoutMillis
    ) {
        checkNotNegative(watermarkThrottlingFrameSize, "watermarkThrottlingFrameSize must be >= 0");
        checkNotNegative(watermarkThrottlingFrameOffset, "watermarkThrottlingFrameOffset must be >= 0");
        checkTrue(watermarkThrottlingFrameOffset < watermarkThrottlingFrameSize || watermarkThrottlingFrameSize == 0,
                "offset must be smaller than frame size");
        checkNotNegative(idleTimeoutMillis, "idleTimeoutMillis must be >= 0 (0 means disabled)");
        this.timestampFn = timestampFn;
        this.newWmPolicyFn = newWmPolicyFn;
        this.wrapFn = wrapFn;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.watermarkThrottlingFrameSize = watermarkThrottlingFrameSize;
        this.watermarkThrottlingFrameOffset = watermarkThrottlingFrameOffset;
    }

    /**
     * Creates and returns a new event time policy. To get a policy that
     * results in no timestamping, call {@link #noEventTime()}.
     *
     * @param timestampFn function that extracts the timestamp from the event;
     *      if null, Jet will use the source's native timestamp
     * @param wrapFn function that transforms the received item and its
     *      timestamp into the emitted item
     * @param newWmPolicyFn factory of the watermark policy objects
     * @param watermarkThrottlingFrameSize the frame length to which we
     *      throttle watermarks, see {@link #watermarkThrottlingFrameSize()}
     * @param watermarkThrottlingFrameOffset the frame offset to which we
     *      throttle watermarks, see {@link #watermarkThrottlingFrameOffset()}
     * @param idleTimeoutMillis the timeout after which a partition will be
     *      marked as <em>idle</em>. Use 0 to disable the feature.
     */
    public static <T> EventTimePolicy<T> eventTimePolicy(
            @Nullable ToLongFunctionEx<? super T> timestampFn,
            @Nonnull ObjLongBiFunction<? super T, ?> wrapFn,
            @Nonnull SupplierEx<? extends WatermarkPolicy> newWmPolicyFn,
            long watermarkThrottlingFrameSize,
            long watermarkThrottlingFrameOffset,
            long idleTimeoutMillis
    ) {
        checkSerializable(timestampFn, "timestampFn");
        checkSerializable(timestampFn, "wrapFn");
        checkSerializable(newWmPolicyFn, "newWmPolicyFn");

        return new EventTimePolicy<>(timestampFn, wrapFn, newWmPolicyFn, watermarkThrottlingFrameSize,
                watermarkThrottlingFrameOffset, idleTimeoutMillis);
    }

    /**
     * Creates and returns a new event time policy. To get a policy that
     * results in no watermarks being emitted, call {@link
     * #noEventTime()}.
     *
     * @param timestampFn function that extracts the timestamp from the event;
     *      if null, Jet will use the source's native timestamp
     * @param newWmPolicyFn factory of the watermark policy objects
     * @param watermarkThrottlingFrameSize the frame length to which we
     *      throttle watermarks, see {@link #watermarkThrottlingFrameSize()}
     * @param watermarkThrottlingFrameOffset the frame offset to which we
     *      throttle watermarks, see {@link #watermarkThrottlingFrameOffset()}
     * @param idleTimeoutMillis the timeout after which a partition will be
     *      marked as <em>idle</em>.
     */
    public static <T> EventTimePolicy<T> eventTimePolicy(
            @Nullable ToLongFunctionEx<? super T> timestampFn,
            @Nonnull SupplierEx<? extends WatermarkPolicy> newWmPolicyFn,
            long watermarkThrottlingFrameSize,
            long watermarkThrottlingFrameOffset,
            long idleTimeoutMillis
    ) {
        return eventTimePolicy(timestampFn, noWrapping(), newWmPolicyFn, watermarkThrottlingFrameSize,
                watermarkThrottlingFrameOffset, idleTimeoutMillis);
    }

    /**
     * Returns an event time policy that results in no timestamping. Only
     * useful in jobs with streaming sources that don't do any aggregation.
     * If there is an aggregation step in the job and you use these parameters,
     * your job will keep accumulating the data without producing any output.
     */
    public static <T> EventTimePolicy<T> noEventTime() {
        return eventTimePolicy(i -> Long.MIN_VALUE, noWrapping(), NO_WATERMARKS, 0, 0, 0);
    }

    @SuppressWarnings("unchecked")
    private static <T> ObjLongBiFunction<T, Object> noWrapping() {
        return (ObjLongBiFunction<T, Object>) NO_WRAPPING;
    }

    /**
     * Returns the function that extracts the timestamp from the event.
     */
    @Nullable
    public ToLongFunctionEx<? super T> timestampFn() {
        return timestampFn;
    }

    /**
     * Returns the function that transforms the received item and its timestamp
     * into the emitted item.
     */
    @Nonnull
    public ObjLongBiFunction<? super T, ?> wrapFn() {
        return wrapFn;
    }

    /**
     * Returns the factory of the watermark policy objects.
     */
    @Nonnull
    public SupplierEx<? extends WatermarkPolicy> newWmPolicyFn() {
        return newWmPolicyFn;
    }

    /**
     * This value together with {@link #watermarkThrottlingFrameOffset()}
     * specify the frame size the watermarks are throttled to. Generally it
     * should match the window slide step used downstream. If there are
     * multiple sliding windows downstream, use the greatest common denominator
     * of them.
     * <p>
     * If this parameter is equal to 0, all watermarks will be suppressed.
     * <p>
     * Technically, a watermark should be emitted after every increase in event
     * time. Because watermarks are broadcast from each processor to all
     * downstream processors, this will bring some overhead. But the watermarks
     * are only needed for window aggregation and only when a window should
     * close, that is at the frame boundary of a sliding window. To reduce the
     * amount of watermarks on the stream, you can configure to emit only those
     * watermarks that would trigger an emission of a new window.
     */
    public long watermarkThrottlingFrameSize() {
        return watermarkThrottlingFrameSize;
    }

    /**
     * See {@link #watermarkThrottlingFrameSize()}
     */
    public long watermarkThrottlingFrameOffset() {
        return watermarkThrottlingFrameOffset;
    }

    /**
     * Returns the amount of time allowed to pass without receiving any events
     * from a partition before marking it as "idle". When the partition
     * becomes idle, the processor emits an {@link
     * com.hazelcast.jet.impl.execution.WatermarkCoalescer#IDLE_MESSAGE} to its
     * output edges. This signals Jet that the watermark can advance as
     * if the partition didn't exist.
     * <p>
     * If you supply a zero or negative value, partitions will never be marked
     * as idle.
     */
    public long idleTimeoutMillis() {
        return idleTimeoutMillis;
    }
}
