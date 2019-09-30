/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.ReachedMaxSizeException;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.internal.util.MapUtil.isNullOrEmpty;

/**
 * This class represents capacity counters for write behind
 * queueing. Basically, we have 2 capacity counters. One
 * is node-wide capacity counter, other one is record-store's
 * reserved capacity counter. Depending on the context
 * we operate on a single counter or both.
 *
 * Note that record-store's reserved capacity counter
 * is only used with transactions and when {@link
 * com.hazelcast.config.MapStoreConfig#writeCoalescing} is off.
 *
 * Every record-store has its own counter instance.
 *
 * @see TxnReservedCapacityCounter
 */
public class TxnReservedCapacityCounterImpl implements TxnReservedCapacityCounter {

    private final ConcurrentMap<UUID, Long> reservedCapacityCountByTxId;
    private final NodeWideUsedCapacityCounter nodeWideUsedCapacityCounter;

    public TxnReservedCapacityCounterImpl(NodeWideUsedCapacityCounter nodeWideUsedCapacityCounter) {
        this.nodeWideUsedCapacityCounter = nodeWideUsedCapacityCounter;
        this.reservedCapacityCountByTxId = new ConcurrentHashMap<>();
    }

    /**
     * Increments 2 counters for supplied txnId.
     * One is record-store's reserved capacity counter and other
     * one is node-wide capacity counter. When node-wide
     * capacity counter is exceeded preconfigured node-wide
     * limit this method throws {@link ReachedMaxSizeException}
     *
     * @throws ReachedMaxSizeException (only when backup is false)
     */
    @Override
    public void increment(UUID txnId, boolean backup) {
        reservedCapacityCountByTxId.compute(txnId, (ignored, currentCapacityCount) -> {
            if (backup) {
                nodeWideUsedCapacityCounter.add(1L);
            } else {
                nodeWideUsedCapacityCounter.addCapacityOrThrowException(1);
            }
            return currentCapacityCount == null ? 1L : (currentCapacityCount + 1L);
        });
    }

    /**
     * Increments 2 counters with reserved capacities per
     * txnId. One is record-store's reserved capacity
     * counter and other one is node-wide capacity counter.
     *
     * Note that this method is only used with migrations
     * and it doesn't throw ReachedMaxSizeException.
     *
     * @param reservedCapacityPerTxnId reserved capacities per
     *                                 txnId
     */
    @Override
    public void copy(Map<UUID, Long> reservedCapacityPerTxnId) {
        if (isNullOrEmpty(reservedCapacityPerTxnId)) {
            return;
        }

        for (Long count : reservedCapacityPerTxnId.values()) {
            nodeWideUsedCapacityCounter.add(count);
        }
        reservedCapacityCountByTxId.putAll(reservedCapacityPerTxnId);
    }

    /**
     * Decrements 2 counters for supplied txnId.
     * One is record-store's reserved capacity counter
     * and other one is node-wide capacity counter.
     *
     * @param txnId id of transaction
     */
    @Override
    public void decrement(UUID txnId) {
        decrement0(txnId, true);
    }

    private void decrement0(UUID txnId, boolean decrementNodeWideCounter) {
        reservedCapacityCountByTxId.computeIfPresent(txnId, (ignored, currentCapacityCount) -> {
            if (decrementNodeWideCounter) {
                nodeWideUsedCapacityCounter.add(-1L);
            }
            return currentCapacityCount == 1L ? null : currentCapacityCount - 1L;
        });
    }

    @Override
    public void decrementOnlyReserved(UUID txnId) {
        decrement0(txnId, false);
    }

    @Override
    public boolean hasReservedCapacity(UUID txnId) {
        if (txnId == null) {
            return false;
        }

        return reservedCapacityCountByTxId.containsKey(txnId);
    }

    @Override
    public void release() {
        for (Long counter : reservedCapacityCountByTxId.values()) {
            nodeWideUsedCapacityCounter.add(-counter);
        }
        reservedCapacityCountByTxId.clear();
    }

    @Override
    public Map<UUID, Long> getReservedCapacityCountPerTxnId() {
        return reservedCapacityCountByTxId;
    }
}
