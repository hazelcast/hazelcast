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

package com.hazelcast.map.impl.mapstore.writebehind;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.UUID;

/**
 * This interface contains methods to increment/decrement
 * reserved capacity of a write behind queue.
 *
 * Actual capacity increment happens on {@link NodeWideUsedCapacityCounter}
 * and it is incremented by all record-stores of all maps on a
 * node. The reason that we have this extra counter is to map
 * used-capacity increments to record-stores. With the implementation
 * of {@link TxnReservedCapacityCounter} we can know which
 * record-store is incremented {@link NodeWideUsedCapacityCounter}.
 * Thus, we can transfer reserved capacity info with migrations.
 *
 * Note that this txn-reserved-capacity-counter is
 * only used with transactional-maps which have
 * non-coalescing write-behind store configuration.
 *
 * Each record-store has its own reserved capacity counter instance.
 */
public interface TxnReservedCapacityCounter {

    /**
     * Used when {@link
     * com.hazelcast.config.MapStoreConfig#writeCoalescing} is enabled.
     */
    TxnReservedCapacityCounter EMPTY_COUNTER = new EmptyTxnReservedCapacityCounter();

    /**
     * Increments capacity count for a transaction.
     *
     * @param txnId  id of transaction
     * @param backup set {@code true} if counter increment
     *               operation is on a backup partition,
     *               otherwise set {@code false}
     */
    void increment(@Nonnull UUID txnId, boolean backup);

    /**
     * Decrements capacity count for a transaction.
     *
     * @param txnId id of transaction
     */
    void decrement(@Nonnull UUID txnId);

    /**
     * Decrements capacity count for a transaction.
     *
     * @param txnId id of transaction
     */
    void decrementOnlyReserved(@Nonnull UUID txnId);

    /**
     * Puts all supplied transactions capacity counts into this counter.
     *
     * @param reservedCapacityPerTxnId reserved capacity counts per txnId
     */
    void putAll(@Nonnull Map<UUID, Long> reservedCapacityPerTxnId);

    /**
     * @param txnId id of transaction
     * @return {@code true} if txnId has reserved
     * capacity, otherwise return {@code false}
     */
    boolean hasReservedCapacity(@Nonnull UUID txnId);

    /**
     * Releases all reserved capacity info on this counter.
     */
    void releaseAllReservations();

    /**
     * Returns reserved capacity counts of all
     * transactions which happened on this record store.
     *
     * @return reserved capacities per txnId
     */
    Map<UUID, Long> getReservedCapacityCountPerTxnId();
}
