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

package com.hazelcast.internal.util;


import java.util.Queue;
import java.util.function.Predicate;

/**
 * Utility class for Queues
 */
public final class QueueUtil {

    private QueueUtil() {
    }

    /**
     * Drains the provided queue by the size of the queue as it is known
     * upfront draining.
     * <p>
     * The rational for using this method is the same as for using
     * {@link Queue#clear()}: to remove all the elements from the queue.
     * There are two major differences to highlight:
     * <ol>
     * <li>This method doesn't guarantee that the queue is empty on
     * return if it is written concurrently.</li>
     * <li>This method returns the number of drained elements, while
     * {@link Queue#clear()} doesn't.</li>
     * </ol>
     * <p>
     * These points makes this method more applicable than
     * {@link Queue#clear()} is in conditions where the queue is written
     * concurrently without blocking the writer threads for the time of
     * draining the queue and the caller needs to know the number of
     * elements removed from the queue.
     *
     * @param queue The queue to be drained
     * @return The number of elements drained from the queue
     */
    public static int drainQueue(Queue<?> queue) {
        return drainQueue(queue, queue.size(), null);
    }

    /**
     * Drains the provided queue by the size of the queue as it is known
     * upfront draining.
     * <p>
     * The rational for using this method is the same as for using
     * {@link Queue#clear()}: to remove all the elements from the queue.
     * There are two major differences to highlight:
     * <ol>
     * <li>This method doesn't guarantee that the queue is empty on
     * return if it is written concurrently.</li>
     * <li>This method returns the number of drained elements, while
     * {@link Queue#clear()} doesn't.</li>
     * </ol>
     * <p>
     * These points makes this method more applicable than
     * {@link Queue#clear()} is in conditions where the queue is written
     * concurrently without blocking the writer threads for the time of
     * draining the queue and the caller needs to know the number of
     * elements removed from the queue.
     * <p>
     * You may provide a predicate which will allow some elements to be drained
     * but not be counted against the returned number of drained events.
     *
     * @param queue              The queue to be drained
     * @param drainedCountFilter filter which determines if the drained element
     *                           is counted against the returned count. The filter
     *                           may be {@code null} in which case all elements
     *                           match
     * @param <E>                the type of the elements in the queue
     * @return The number of elements drained from the queue which pass the
     * given predicate
     */
    public static <E> int drainQueue(Queue<E> queue, Predicate<E> drainedCountFilter) {
        return drainQueue(queue, queue.size(), drainedCountFilter);
    }


    /**
     * Drains the provided queue by the count provided in {@code elementsToDrain}.
     * You may provide a predicate which will allow some elements to be drained
     * but not be counted against the returned number of drained events.
     *
     * @param queue              The queue to be drained
     * @param elementsToDrain    The number of elements to be drained from
     *                           the queue
     * @param drainedCountFilter filter which determines if the drained element
     *                           is counted against the returned count. The filter
     *                           may be {@code null} in which case all elements
     *                           match
     * @param <E>                the type of the elements in the queue
     * @return The number of elements drained from the queue which pass the
     * given predicate
     */
    public static <E> int drainQueue(Queue<E> queue,
                                     int elementsToDrain,
                                     Predicate<E> drainedCountFilter) {
        int drained = 0;
        boolean drainMore = true;
        for (int i = 0; i < elementsToDrain && drainMore; i++) {
            E polled = queue.poll();
            if (polled != null) {
                if (drainedCountFilter == null || drainedCountFilter.test(polled)) {
                    drained++;
                }
            } else {
                drainMore = false;
            }
        }

        return drained;
    }
}
