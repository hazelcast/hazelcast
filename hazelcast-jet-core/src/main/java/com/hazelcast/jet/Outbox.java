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

package com.hazelcast.jet;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * Data sink for a {@link Processor}. The outbox consists of individual
 * output buckets, one per outbound edge of the vertex represented by
 * the associated processor. The processor must deliver its output items,
 * separated by destination edge, into the outbox by calling
 * {@link #offer(int, Object)} or {@link #offer(Object)}.
 * <p>
 * In the case of a processor declared as <em>cooperative</em>, the
 * execution engine will not try to flush the outbox into downstream
 * queues until the processing method returns. Therefore the processor
 * must check the return value of {@code offer()} and refrain from
 * outputting more data when it returns {@code false}.
 * <p>
 * A non-cooperative processor's outbox will have auto-flushing behavior
 * and each item will be immediatelly flushed to the edge, blocking as
 * needed until success.
 */
public interface Outbox {

    /**
     * Returns the number of buckets in this outbox.
     */
    int bucketCount();

    /**
     * Offers the supplied item to the output bucket with the supplied
     * ordinal. If {@code ordinal == -1}, offers the supplied item to
     * all buckets (behaves the same as {@link #offer(Object)}).
     * If any of the involved buckets is full, takes no action and
     * returns {@code false}.
     *
     * @return whether the outbox accepted the item
     */
    @CheckReturnValue
    boolean offer(int ordinal, @Nonnull Object item);

    /**
     * First ensures that all the buckets identified in the supplied
     * array of ordinals have room for another item, then adds the
     * supplied item to them. If any of the buckets is full, takes no
     * action and returns {@code false}.
     *
     * @return whether the outbox accepted the item
     */
    @CheckReturnValue
    boolean offer(int[] ordinals, @Nonnull Object item);

    /**
     * First ensures that all buckets have room for another item, then
     * adds the supplied item to them. If any of the buckets is full,
     * takes no action and returns {@code false}.
     *
     * @return whether the outbox accepted the item
     */
    @CheckReturnValue
    default boolean offer(@Nonnull Object item) {
        return offer(-1, item);
    }
}
