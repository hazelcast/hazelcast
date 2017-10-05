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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * Data sink for a {@link Processor}. The outbox consists of individual
 * output buckets, one per outbound edge of the vertex represented by the
 * associated processor and one for the snapshot state.
 * The processor must deliver its output items separated by destination
 * edge, into the outbox by calling {@link #offer(int, Object)} or
 * {@link #offer(Object)}. The items for the snapshot state can be delivered
 * via {@link #offerToSnapshot(Object, Object)} during
 * calls to {@link Processor#saveToSnapshot() saveToSnapshot()}.
 * <p>
 * Outbox might not be able to accept the item if it is already full. The
 * processor must check the return value of {@code offer()} and refrain from
 * outputting more data if it returns {@code false}.
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
     * Offers the specified key and value pair to the processor's snapshot storage.
     * <p>
     * During a snapshot restore the type of key offered determines which processors
     * receive the key and value pair. If the key is of type {@link BroadcastKey},
     * the entry will be restored to all processor instances.
     * Otherwise, the key will be distributed according to default partitioning and
     * only a single processor instance will receive the key.
     *
     * The methods in this class may only be called from inside the
     * {@link Processor#saveToSnapshot()} method.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value);

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
