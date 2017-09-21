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
 * output buckets, one per outbound edge of the vertex represented by the
 * associated processor. The processor must deliver its output items, separated by destination edge,
 * into the outbox by calling {@link #offer(int, Object)} or {@link
 * #offer(Object)}.
 * <p>
 * A {@link Processor#isCooperative() cooperative} processor's outbox might
 * not be able to accept the item if it is already full. The processor must
 * check the return value of {@code offer()} and refrain from outputting
 * more data when it returns {@code false}.
 * <p>
 * A non-cooperative processor's outbox will block until the item can fit into
 * the downstream buffers and the {@code offer} methods will always return
 * {@code true}.
 */
public interface Outbox {

    /**
     * Returns the number of buckets in this outbox. This is equal to the
     * number of output edges of the vertex.
     */
    int bucketCount();

    /**
     * Offers the supplied item to the downstream edge with the supplied
     * ordinal. If {@code ordinal == -1}, offers the supplied item to
     * all edges (behaves the same as {@link #offer(Object)}).
     * <p>
     * If any downstream queue is full, it is skipped and this method returns
     * {@code false}. In that case the call must be retried later with the same
     * (or equal) item. The outbox internally keeps track which queues already
     * accepted the item.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    boolean offer(int ordinal, @Nonnull Object item);

    /**
     * Offers the item to all supplied edge ordinals. See {@link #offer(int,
     * Object)} for more details.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    boolean offer(int[] ordinals, @Nonnull Object item);

    /**
     * Offers the item to all edges. See {@link #offer(int, Object)} for more
     * details.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    default boolean offer(@Nonnull Object item) {
        return offer(-1, item);
    }

}
