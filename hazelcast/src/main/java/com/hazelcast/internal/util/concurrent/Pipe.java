/*
 * Original work Copyright 2015 Real Logic Ltd.
 * Modified work Copyright (c) 2015-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util.concurrent;

import java.util.Collection;
import java.util.function.Predicate;

/**
 * A container of items processed in sequence. The point of this interface
 * to be a mix-in to the API of JDK's standard {@link java.util.Queue} adding
 * methods that make sense for a queue which is concurrent, bounded, and
 * supports draining in batches.
 *
 * @param <E> type of items in the pipe.
 */
public interface Pipe<E> {
    /**
     * Returns the number of items added to this pipe since creation.
     */
    long addedCount();

    /**
     * Returns the number of items removed from this pipe since creation.
     */
    long removedCount();

    /**
     * Returns the number of items this pipe can hold at a time.
     */
    int capacity();

    /**
     * Returns the number of items this pipe has still room for.
     */
    int remainingCapacity();

    /**
     * Drains the items available in the pipe to the supplied item handler.
     * The handler returns a {@code boolean} which decides whether to continue
     * draining. If it returns {@code false}, this method refrains from draining
     * further items and returns.
     * <p>
     * <i>Implementation note:</i> this method is expected to take advantage of
     * the fact that many items are being drained at once and minimize the
     * per-item cost of housekeeping.
     *
     * @param itemHandler the item handler
     * @return the number of drained items
     */
    int drain(Predicate<? super E> itemHandler);

    /**
     * Drains at most {@code limit} available items into the provided
     * {@link Collection}.
     * <p>
     * <i>Implementation note:</i> this method is expected to take advantage of
     * the fact that many items are being drained at once and minimize the
     * per-item cost of housekeeping.
     *
     * @param target destination for the drained items
     * @param limit  the maximum number of items to drain
     * @return the number of drained items
     */
    int drainTo(Collection<? super E> target, int limit);
}
