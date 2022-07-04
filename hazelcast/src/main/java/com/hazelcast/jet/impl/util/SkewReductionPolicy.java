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

package com.hazelcast.jet.impl.util;

import com.hazelcast.jet.JetException;

import java.util.Arrays;

import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.jet.impl.util.Util.subtractClamped;

/**
 * A stream's <i>skew</i> is defined on a set of its substreams that are
 * being consumed in parallel: given the set of the watermark values of
 * each substream, it is the difference between the top and the bottom
 * value in that set. Stream skew has detrimental effects on at least one
 * of:
 * <ol><li>
 *     memory consumption
 * </li><li>
 *     throughput
 * </li><li>
 *     latency
 * </li><li>
 *     correctness
 * </li></ol>
 * This class implements a policy to reduce the stream skew by managing the
 * priority of draining individual substream queues. As soon as a queue's
 * skew (the difference between its watermark and the bottom watermark)
 * reaches the configured {@code priorityDrainingThreshold}, it will be
 * drained only if no queue with a lower skew had any data. Furthermore,
 * when a queue exceeds the configured {@code maxSkew}, action will be
 * taken to prevent its further increase by either:
 * <ul><li>
 *     refusing to drain the advanced queues while waiting for the watermark
 *     on the lagging queues to catch up until the skew is back below
 *     {@code maxSkew}, or
 * </li><li>
 *     force-advancing the watermark beyond the lagging queues' watermark,
 *     possibly causing items on those queues that wouldn't be late by their
 *     local watermark, to be declared late anyway and dropped.
 * </li></ul>
 * This class should be used in the loop that drains the queues, as
 * follows:
 * <ol><li>
 *     Create an instance of this class, supplying the number of queues the
 *     loop will be draining.
 * </li><li>
 *     Use an indexed loop ranging over {@code [0..numQueues)}.
 * </li><li>
 *     For each {@code i} prepare to drain the queue indicated by {@link
 *     #toQueueIndex( int) toQueueIndex(i)}.
 * </li><li>
 *     Before draining the queue check the result of {@link
 *     #shouldStopDraining(int, boolean) shouldStopDraining(drainOrder,
 *     madeProgress)} to see whether to exit the loop.
 * </li><li>
 *     Call {@link #observeWm(int, long) observeWm(queueIndex, wmValue)}
 *     for every watermark item received from any queue. If this method
 *     returns {@code true}, it means that the draining order was changed and
 *     the draining loop should exit.
 * </li></ol>
 */
public class SkewReductionPolicy {

    // package-visible for tests
    final long[] queueWms;
    final int[] drainOrderToQIdx;

    private final long maxSkew;
    private final long priorityDrainingThreshold;
    private final boolean forceAdvanceWm;

    /**
     * Creates a policy which does not stop draining from any queue under any
     * condition and does not advance watermark eagerly. The policy only sets
     * the draining order for the queues by ordering them by their current
     * watermark value. The queue with the lowest watermark value will have the
     * drain order 0.
     */
    public SkewReductionPolicy(int numQueues) {
        this(numQueues, Long.MAX_VALUE, Long.MAX_VALUE, false);
    }

    public SkewReductionPolicy(int numQueues, long maxSkew, long priorityDrainingThreshold, boolean forceAdvanceWm) {
        checkNotNegative(maxSkew, "maxSkew must not be a negative number");
        checkNotNegative(priorityDrainingThreshold, "priorityDrainingThreshold must not be a negative number");
        checkTrue(priorityDrainingThreshold <= maxSkew, "priorityDrainingThreshold must be less than maxSkew");

        this.maxSkew = maxSkew;
        this.priorityDrainingThreshold = priorityDrainingThreshold;
        this.forceAdvanceWm = forceAdvanceWm;

        queueWms = new long[numQueues];
        Arrays.fill(queueWms, Long.MIN_VALUE);

        drainOrderToQIdx = new int[numQueues];
        Arrays.setAll(drainOrderToQIdx, i -> i);
    }

    /**
     * Given the (variable) position of a queue in the draining order, returns
     * the (fixed) index of that queue in the array of all queues. Queues are
     * ordered for draining by their watermark (lowest first) so that the
     * least advanced queue is drained first.
     */
    public int toQueueIndex(int drainOrderIdx) {
        return drainOrderToQIdx[drainOrderIdx];
    }

    /**
     * Called to report the value of watermark observed on the queue at
     * {@code queueIndex}.
     *
     * @return {@code true} if the queues were reordered by this watermark
     */
    public boolean observeWm(int queueIndex, final long wmValue) {
        if (queueWms[queueIndex] >= wmValue) {
            // this is possible if force-advancing the watermark because we increase
            // the queueWmValue without receiving watermark from that queue
            if (!forceAdvanceWm) {
                throw new JetException("Watermarks not monotonically increasing on queue: " +
                        "last one=" + queueWms[queueIndex] + ", new one=" + wmValue);
            }
            return false;
        }
        boolean didReorder = adjustDrainingOrder(queueIndex, wmValue);
        queueWms[queueIndex] = wmValue;
        forceAdvanceWmIfConfigured();
        return didReorder;
    }

    private void forceAdvanceWmIfConfigured() {
        if (!forceAdvanceWm) {
            return;
        }
        long newBottomWm = subtractClamped(topObservedWm(), maxSkew);
        for (int i = 0; i < drainOrderToQIdx.length && queueWms[drainOrderToQIdx[i]] < newBottomWm; i++) {
            queueWms[drainOrderToQIdx[i]] = newBottomWm;
        }
    }

    /**
     * The queue-draining loop drains the queues in the order specified by this
     * class and consults this method before going on to drain the next queue.
     * The method determines the skew of the queue the loop is about to drain:
     * it is the difference between its watermark and the bottom watermark
     * (i.e., that of the first queue in the draining order). The policy will
     * signal to stop draining if:
     * <ol><li>
     *     some data was already drained and the queue has exceeded the
     *     configured "priority draining" skew threshold, or
     * </li><li>
     *     the queue has exceeded the configured {@code maxSkew} and the
     *     configured policy is not to force-advance the watermark.
     * </li></ol>
     *
     * @param queueIndex the current position in the draining order
     * @param madeProgress whether any queue was drained so far
     * @return {@code false} if the draining should now stop; {@code true} otherwise
     */
    public boolean shouldStopDraining(int queueIndex, boolean madeProgress) {
        long skew = subtractClamped(queueWms[queueIndex], queueWms[drainOrderToQIdx[0]]);
        return (madeProgress && skew > priorityDrainingThreshold) || (!forceAdvanceWm && skew > maxSkew);
    }

    public long bottomObservedWm() {
        return queueWms[drainOrderToQIdx[0]];
    }

    private long topObservedWm() {
        return queueWms[drainOrderToQIdx[drainOrderToQIdx.length - 1]];
    }

    /**
     * Reacts to the advancement of a queue's watermark by repositioning
     * it in the drain order, so as to keep the drain order sorted by
     * queue watermark.
     *
     * @return whether the queue had to be repositioned
     */
    private boolean adjustDrainingOrder(int queueIndex, long wmValue) {
        int currPos = findCurrentDrainPos(queueIndex);
        int newPos = findNewDrainPos(currPos, wmValue);
        if (newPos == currPos) {
            return false;
        }
        assert newPos > currPos : "newPos < currPos";
        System.arraycopy(drainOrderToQIdx, currPos + 1, drainOrderToQIdx, currPos, newPos - currPos);
        drainOrderToQIdx[newPos] = queueIndex;
        return true;
    }

    private int findCurrentDrainPos(int queueIndex) {
        for (int i = 0; i < drainOrderToQIdx.length; i++) {
            if (drainOrderToQIdx[i] == queueIndex) {
                return i;
            }
        }
        throw new AssertionError(
                "Failed to find the queue index " + queueIndex + " in the drainOrder->queueIndex lookup table");
    }

    private int findNewDrainPos(int currPos, long queueWm) {
        int i = currPos + 1;
        for (; i < drainOrderToQIdx.length; i++) {
            if (queueWms[drainOrderToQIdx[i]] >= queueWm) {
                break;
            }
        }
        // the new position is before the greater-or-equal item
        return i - 1;
    }
}
