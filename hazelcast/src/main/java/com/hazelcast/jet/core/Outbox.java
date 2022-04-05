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

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;

/**
 * Data sink for a {@link Processor}. The outbox consists of individual
 * output buckets, one per outbound edge of the vertex represented by the
 * associated processor and one for the snapshot state. The processor must
 * deliver its output items separated by destination edge into the outbox
 * by calling {@link #offer(int, Object)} or {@link #offer(Object)}.
 * <p>
 * To save its current state to the snapshot, it must call {@link
 * #offerToSnapshot(Object, Object)} from its implementation of {@link
 * Processor#saveToSnapshot() saveToSnapshot()}.
 * <p>
 * The outbox has finite capacity and will eventually refuse an item. If
 * one of the {@code offer()} methods returns {@code false}, the calling
 * processor must return from its callback method and retry delivering the
 * same item when Jet calls its method again.
 *
 * <h4>Thread safety</h4>
 * You must offer to outbox only from the thread that called the processor's
 * methods. For example, you must not offer to outbox from a callback of an
 * asynchronous operation. If you need that, you have to employ a concurrent
 * queue, add to it in the callback and drain it in e.g. {@link
 * Processor#tryProcess()}.
 *
 * @since Jet 3.0
 */
public interface Outbox {

    /**
     * Returns the number of buckets in this outbox. This is equal to the
     * number of output edges of the vertex and does not include the snapshot
     * bucket.
     */
    int bucketCount();

    /**
     * Offers the supplied item to the bucket with the supplied ordinal.
     * <p>
     * Items offered to outbox should not be subsequently mutated because the
     * same instance might be used by a downstream processor in different
     * thread, causing concurrent access.
     * <p>
     * Outbox is not thread safe, see {@link Outbox Thread safety} in its class
     * javadoc.
     *
     * @param ordinal output ordinal number or -1 to offer to all ordinals
     * @return {@code true} if the outbox accepted the item
     */
    @CheckReturnValue
    boolean offer(int ordinal, @Nonnull Object item);

    /**
     * Offers the item to all supplied edge ordinals. See {@link #offer(int,
     * Object)} for more details.
     * <p>
     * Outbox is not thread safe, see {@link Outbox Thread safety} in its class
     * javadoc.
     *
     * @return {@code true} if the outbox accepted the item
     */
    @CheckReturnValue
    boolean offer(@Nonnull int[] ordinals, @Nonnull Object item);

    /**
     * Offers the given key and value pair to the processor's snapshot
     * storage.
     * <p>
     * The type of the offered key determines which processors receive the key
     * and value pair when it is restored. If the key is of type {@link
     * BroadcastKey}, the entry will be restored to all processor instances.
     * Otherwise the key will be distributed according to default partitioning
     * and only a single processor instance will receive the key.
     * <p>
     * This method must only be called from the {@link
     * Processor#saveToSnapshot()} or {@link Processor#snapshotCommitPrepare()}
     * methods.
     * <p>
     * Keys and values offered to snapshot are serialized and can be further
     * mutated as soon as this method returns.
     * <p>
     * Outbox is not thread safe, see {@link Outbox Thread safety} in its class
     * javadoc.
     *
     * @return {@code true} if the outbox accepted the item
     */
    @CheckReturnValue
    boolean offerToSnapshot(@Nonnull Object key, @Nonnull Object value);

    /**
     * Offers the item to all edges. See {@link #offer(int, Object)} for more
     * details.
     * <p>
     * Outbox is not thread safe, see {@link Outbox Thread safety} in its class
     * javadoc.
     *
     * @return {@code true} if the outbox accepted the item
     */
    @CheckReturnValue
    default boolean offer(@Nonnull Object item) {
        return offer(-1, item);
    }

    /**
     * Returns true if this outbox has an unfinished item and the same item
     * must be offered again. If it returns false, it is safe to offer a new
     * item.
     * <p>
     * Outbox is not thread safe, see {@link Outbox Thread safety} in its class
     * javadoc.
     */
    boolean hasUnfinishedItem();
}
