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

package com.hazelcast.jet.impl.execution;

import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.jet.JetException;
import com.hazelcast.jet.core.Watermark;

import java.util.Arrays;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;

/**
 * Implements {@link Watermark} coalescing. Tracks WMs on input queues and
 * decides when to forward the WM. The watermark should be forwarded when
 * it has been received from all input streams (ignoring idle streams).
 *
 * The class also handles idle messages from inputs (coming in the form of a
 * watermark equal to {@link #IDLE_MESSAGE}). When such a message is received,
 * that input is switched to <em>idle</em> and excluded from coalescing. Any
 * event or watermark from such input will turn the input back to
 * <em>active</em> state.
 */
public abstract class WatermarkCoalescer {

    public static final Watermark IDLE_MESSAGE = new Watermark(Long.MAX_VALUE);

    static final long NO_NEW_WM = Long.MIN_VALUE;

    private WatermarkCoalescer() { }

    /**
     * Called when the queue with the given index is exhausted.
     *
     * @return the watermark value to emit or {@link #NO_NEW_WM} if no
     * watermark should be forwarded
     */
    public abstract long queueDone(int queueIndex);

    /**
     * Called after receiving a new event. Will change the queue state to
     * <em>active</em>.
     *
     * @param queueIndex index of the queue on which the event was received.
     */
    public abstract void observeEvent(int queueIndex);

    /**
     * Called after receiving a new watermark.
     *
     * @param queueIndex index of the queue on which the WM was received.
     * @param wmValue    the watermark value, it can be {@link #IDLE_MESSAGE}
     * @return the watermark value to emit or {@link #NO_NEW_WM} if no
     *      watermark should be forwarded. It can return {@link #IDLE_MESSAGE}
     */
    public abstract long observeWm(int queueIndex, long wmValue);

    /**
     * Checks if there is a watermark to emit now based on the passage of
     * system time or if all input queues are idle and we should forward the
     * idle marker.
     *
     * @return the watermark value to emit, {@link #IDLE_MESSAGE} or
     *      {@link #NO_NEW_WM} if no watermark should be forwarded
     */
    public abstract long checkWmHistory();

    /**
     * Returns the last emitted watermark.
     */
    public abstract long coalescedWm();

    /**
     * Returns the highest received watermark from any input.
     */
    public abstract long topObservedWm();

    /**
     * Factory method.
     *
     * @param queueCount number of queues
     */
    public static WatermarkCoalescer create(int queueCount) {
        checkNotNegative(queueCount, "queueCount must be >= 0, but is " + queueCount);
        switch (queueCount) {
            case 0:
                return new ZeroInputImpl();
            case 1:
                return new SingleInputImpl();
            default:
                return new StandardImpl(queueCount);
        }
    }

    /**
     * Special-case implementation for zero inputs (source processors).
     */
    private static final class ZeroInputImpl extends WatermarkCoalescer {

        @Override
        public void observeEvent(int queueIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long observeWm(int queueIndex, long wmValue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long queueDone(int queueIndex) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long checkWmHistory() {
            return NO_NEW_WM;
        }

        @Override
        public long coalescedWm() {
            return Long.MIN_VALUE;
        }

        @Override
        public long topObservedWm() {
            return Long.MIN_VALUE;
        }
    }

    /**
     * Special-case implementation for single input (i.e. no coalescing, just
     * forwarding).
     */
    private static final class SingleInputImpl extends WatermarkCoalescer {

        private final Counter queueWm = SwCounter.newSwCounter(Long.MIN_VALUE);

        @Override
        public long queueDone(int queueIndex) {
            assert queueWm.get() < Long.MAX_VALUE : "Duplicate DONE call";
            queueWm.set(Long.MAX_VALUE);
            return NO_NEW_WM;
        }

        @Override
        public void observeEvent(int queueIndex) {
        }

        @Override
        public long observeWm(int queueIndex, long wmValue) {
            assert queueIndex == 0 : "queueIndex=" + queueIndex;
            if (queueWm.get() >= wmValue) {
                throw new JetException("Watermarks not monotonically increasing on queue: " +
                        "last one=" + queueWm + ", new one=" + wmValue);
            }
            if (wmValue != IDLE_MESSAGE.timestamp()) {
                queueWm.set(wmValue);
            }
            return wmValue;
        }

        @Override
        public long checkWmHistory() {
            return NO_NEW_WM;
        }

        @Override
        public long coalescedWm() {
            return queueWm.get();
        }

        @Override
        public long topObservedWm() {
            return queueWm.get();
        }
    }

    /**
     * Standard implementation for 1..n inputs.
     */
    private static final class StandardImpl extends WatermarkCoalescer {

        private final long[] queueWms;
        private final boolean[] isIdle;
        private final Counter lastEmittedWm = SwCounter.newSwCounter(Long.MIN_VALUE);
        private final Counter topObservedWm = SwCounter.newSwCounter(Long.MIN_VALUE);
        private boolean allInputsAreIdle;
        private boolean idleMessagePending;

        StandardImpl(int queueCount) {
            isIdle = new boolean[queueCount];
            queueWms = new long[queueCount];
            Arrays.fill(queueWms, Long.MIN_VALUE);
        }

        @Override
        public long queueDone(int queueIndex) {
            assert queueWms[queueIndex] < Long.MAX_VALUE : "Duplicate DONE call";
            queueWms[queueIndex] = Long.MAX_VALUE;
            return checkObservedWms();
        }

        @Override
        public void observeEvent(int queueIndex) {
            if (isIdle[queueIndex]) {
                isIdle[queueIndex] = false;
                allInputsAreIdle = false;
            }
        }

        @Override
        public long observeWm(int queueIndex, long wmValue) {
            if (queueWms[queueIndex] >= wmValue) {
                throw new JetException("Watermarks not monotonically increasing on queue: " +
                        "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
            }

            if (wmValue == IDLE_MESSAGE.timestamp()) {
                isIdle[queueIndex] = true;
            } else {
                isIdle[queueIndex] = false;
                allInputsAreIdle = false;
                queueWms[queueIndex] = wmValue;
                if (wmValue > topObservedWm.get()) {
                    topObservedWm.set(wmValue);
                }
            }
            return checkObservedWms();
        }

        private long checkObservedWms() {
            if (allInputsAreIdle) {
                // we've already returned IDLE_MESSAGE, let's do nothing now
                return NO_NEW_WM;
            }

            // find lowest observed wm
            long min = Long.MAX_VALUE;
            int notDoneInputCount = 0;
            for (int i = 0; i < queueWms.length; i++) {
                if (queueWms[i] < Long.MAX_VALUE) {
                    notDoneInputCount++;
                }
                if (!isIdle[i] && queueWms[i] < min) {
                    min = queueWms[i];
                }
            }

            // if the lowest observed wm is MAX_VALUE that means that all inputs are idle or done
            if (min == Long.MAX_VALUE) {
                // When all inputs are idle, we should first emit top observed WM.
                // For example: have 2 queues. Q1 got to wm(1), Q2 to wm(2). Later on, both become idle at the
                // same moment. Now two things can happen:
                //   1. Idle-message from Q1 is received first: Q1 is excluded from coalescing, wm(2) is forwarded.
                //      Then message from Q2 is received, WM stays at wm(2)
                //   2. Idle-message from Q2 is received first: Q2 is excluded from coalescing, WM stays at wm(1).
                //      Then message from Q1 is received. Without this condition WM would stay at wm(1). With it,
                //      wm(2) is forwarded.
                allInputsAreIdle = true;
                final long topObservedWmLocal = topObservedWm.get();
                if (topObservedWmLocal > lastEmittedWm.get()) {
                    idleMessagePending = notDoneInputCount != 0;
                    lastEmittedWm.set(topObservedWmLocal);
                    return topObservedWmLocal;
                }
                return notDoneInputCount != 0
                        ? IDLE_MESSAGE.timestamp()
                        : NO_NEW_WM;
            }

            // if the new lowest observed wm is larger than already emitted, emit it
            if (min > lastEmittedWm.get()) {
                lastEmittedWm.set(min);
                return min;
            }

            return NO_NEW_WM;
        }

        @Override
        public long checkWmHistory() {
            if (idleMessagePending) {
                idleMessagePending = false;
                return IDLE_MESSAGE.timestamp();
            }
            return NO_NEW_WM;
        }

        @Override
        public long coalescedWm() {
            return lastEmittedWm.get();
        }

        @Override
        public long topObservedWm() {
            return topObservedWm.get();
        }
    }
}
