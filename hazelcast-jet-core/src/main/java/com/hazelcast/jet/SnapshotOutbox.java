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

import static com.hazelcast.jet.BroadcastKey.broadcastKey;

/**
 * A {@link Outbox} which is used for offering items to processor snapshot state.
 * <p>
 * The methods on this class may only be called from inside the
 * {@link Processor#saveSnapshot()} method.
 * <p>
 * As with the regular {@link Outbox}, anon-cooperative processor's outbox will
 * block until the item can fit into the downstream buffers
 * and the {@code offer} methods will always return
 * {@code true}.
 */
public interface SnapshotOutbox {

    /**
     * Offers the specified key and value pair to the processor's snapshot storage.
     * State stored this way once restored will be distributed between all
     * processors instances using default partitioning.
     * <p>
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    boolean offer(Object key, Object value);

    /**
     * Offers the specified key and value pair to the processor's snapshot storage.
     * State stored this way once restored will be broadcast to all
     * processors instances, meaning all processor instances will receive
     * all of the broadcast key and value pairs.
     *
     * @return whether the outbox fully accepted the item
     */
    @CheckReturnValue
    default boolean offerBroadcast(Object key, Object value) {
        return offer(broadcastKey(key), value);
    }
}
