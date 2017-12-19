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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;
import com.hazelcast.jet.impl.util.TimestampHistory;

import java.util.Arrays;

import static com.hazelcast.util.Preconditions.checkNotNegative;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Implements {@link Watermark} coalescing. Tracks WMs on queues and decides
 * when to forward the WM. The watermark should be forwarded:
 * <ul>
 *     <li>when it has been received from all input streams
 *     <li>if the maximum watermark retention time has elapsed
 * </ul>
 * <p>
 * There's no separate unit test for this class, it's tested as a part of
 * {@link ConcurrentInboundEdgeStream}.
 */
abstract class WatermarkCoalescer {

    long lastEmittedWm = Long.MIN_VALUE;

    /**
     * Called when the queue with the given index is exhausted.
     *
     * @return the watermark value to emit or {@code Long.MIN_VALUE} if no watermark
     * should be emitted
     */
    public abstract long queueDone(int queueIndex);

    /**
     * Called after receiving a new watermark.
     *
     * @param systemTime current system time
     * @param queueIndex index of queue on which the WM was received.
     * @param wmValue    the watermark value
     * @return the watermark value to emit or {@code Long.MIN_VALUE}
     */
    public abstract long observeWm(long systemTime, int queueIndex, long wmValue);

    /**
     * Checks if there is a watermark to emit now based on the passage of
     * system time.
     *
     * @param systemTime Current system time
     * @return Watermark timestamp to emit or {@code Long.MIN_VALUE}
     */
    public abstract long checkWmHistory(long systemTime);

    /**
     * Returns {@code System.nanoTime()} or a dummy value, if it is not needed,
     * because the call is expensive in hot loop.
     */
    public abstract long getTime();

    public static WatermarkCoalescer create(int maxWatermarkRetainMillis, int queueCount) {
        checkNotNegative(queueCount, "queueCount must be >= 0, but is " + queueCount);
        switch (queueCount) {
            case 0:
                return new ZeroInputImpl();
            case 1:
               return new SingleInputImpl();
            default:
                return new StandardImpl(maxWatermarkRetainMillis, queueCount);
        }
    }

    private static final class ZeroInputImpl extends WatermarkCoalescer {
        @Override
        public long queueDone(int queueIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long observeWm(long systemTime, int queueIndex, long wmValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long checkWmHistory(long systemTime) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getTime() {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Simple implementation for single input, when there's really nothing to
     * coalesce.
     */
    private static final class SingleInputImpl extends WatermarkCoalescer {
        @Override
        public long queueDone(int queueIndex) {
            assert queueIndex == 0 : "queueIndex=" + queueIndex;
            return Long.MIN_VALUE;
        }

        @Override
        public long observeWm(long systemTime, int queueIndex, long wmValue) {
            assert queueIndex == 0 : "queueIndex=" + queueIndex;
            if (lastEmittedWm < wmValue) {
                lastEmittedWm = wmValue;
                return wmValue;
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long checkWmHistory(long systemTime) {
            return Long.MIN_VALUE;
        }

        @Override
        public long getTime() {
            return -1;
        }
    }

    /**
     * Standard implementation for multiple queues.
     */
    private static final class StandardImpl extends WatermarkCoalescer {

        private final long[] queueWms;
        private final TimestampHistory watermarkHistory;

        private long topObservedWm = Long.MIN_VALUE;

        StandardImpl(int maxWatermarkRetainMillis, int queueCount) {
            queueWms = new long[queueCount];
            Arrays.fill(queueWms, Long.MIN_VALUE);

            watermarkHistory = maxWatermarkRetainMillis >= 0
                    ? new TimestampHistory(MILLISECONDS.toNanos(maxWatermarkRetainMillis))
                    : null;
        }

        @Override
        public long queueDone(int queueIndex) {
            queueWms[queueIndex] = Long.MAX_VALUE;

            long bottomVm = bottomObservedWm();
            if (bottomVm > lastEmittedWm && bottomVm != Long.MAX_VALUE) {
                lastEmittedWm = bottomVm;
                return bottomVm;
            }

            return Long.MIN_VALUE;
        }

        @Override
        public long observeWm(long systemTime, int queueIndex, long wmValue) {
            if (queueWms[queueIndex] >= wmValue) {
                throw new JetException("Watermarks not monotonically increasing on queue: " +
                        "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
            }
            queueWms[queueIndex] = wmValue;

            long wmToEmit = Long.MIN_VALUE;

            if (watermarkHistory != null && wmValue > topObservedWm) {
                topObservedWm = wmValue;
                wmToEmit = watermarkHistory.sample(systemTime, topObservedWm);
            }

            wmToEmit = Math.max(wmToEmit, bottomObservedWm());
            if (wmToEmit > lastEmittedWm) {
                lastEmittedWm = wmToEmit;
                return wmToEmit;
            }

            return Long.MIN_VALUE;
        }

        @Override
        public long checkWmHistory(long systemTime) {
            if (watermarkHistory == null) {
                return Long.MIN_VALUE;
            }
            long historicWm = watermarkHistory.sample(systemTime, topObservedWm);
            if (historicWm > lastEmittedWm) {
                lastEmittedWm = historicWm;
                return historicWm;
            }
            return Long.MIN_VALUE;
        }

        @Override
        public long getTime() {
            return watermarkHistory != null ? System.nanoTime() : -1;
        }

        private long bottomObservedWm() {
            long min = queueWms[0];
            for (int i = 1; i < queueWms.length; i++) {
                if (queueWms[i] < min) {
                    min = queueWms[i];
                }
            }
            return min;
        }
    }
}
