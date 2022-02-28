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

package com.hazelcast.internal.crdt.pncounter;

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.core.ConsistencyLostException;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.internal.crdt.CRDT;
import com.hazelcast.internal.crdt.CRDTDataSerializerHook;
import com.hazelcast.crdt.MutationDisallowedException;
import com.hazelcast.internal.crdt.pncounter.operations.CRDTTimestampedLong;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.MapUtil;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * PN (Positive-Negative) CRDT counter where each replica is identified by
 * a String.
 */
public class PNCounterImpl implements CRDT<PNCounterImpl>, IdentifiedDataSerializable {
    private UUID localReplicaId;
    /** Name of this PN counter */
    private String name;
    /** The PN counter state */
    private Map<UUID, long[]> state = new ConcurrentHashMap<>();
    /** Vector clock of updates reflected in the state of this replica */
    private VectorClock stateVectorClock = new VectorClock();
    /** Flag marking this CRDT as migrated. No updates are allowed to a migrated CRDT */
    private volatile boolean migrated;
    /** The locks for reading and writing the PN counter state */
    private final ReadWriteLock stateReadWriteLock = new ReentrantReadWriteLock();
    private final Lock stateReadLock = stateReadWriteLock.readLock();
    private final Lock stateWriteLock = stateReadWriteLock.writeLock();

    PNCounterImpl(UUID localReplicaId, String name) {
        this.localReplicaId = localReplicaId;
        this.stateVectorClock.setReplicaTimestamp(localReplicaId, Long.MIN_VALUE);
        this.name = name;
    }

    public PNCounterImpl() {
    }

    /**
     * Returns the current value of the counter.
     * <p>
     * The method can throw a {@link ConsistencyLostException} when the state
     * of this CRDT is not causally related to the observed timestamps. This
     * means that it cannot provide the session guarantees of RYW (read your
     * writes) and monotonic read.
     *
     * @param observedTimestamps the vector clock last observed by the client of
     *                           this counter
     * @return the current counter value with the current counter vector clock
     * @throws ConsistencyLostException if this replica cannot provide the
     *                                  session guarantees
     */
    public CRDTTimestampedLong get(VectorClock observedTimestamps) {
        checkSessionConsistency(observedTimestamps);
        stateReadLock.lock();
        try {
            long value = 0;
            for (long[] pnValue : state.values()) {
                value += pnValue[0];
                value -= pnValue[1];
            }
            return new CRDTTimestampedLong(value, new VectorClock(stateVectorClock));
        } finally {
            stateReadLock.unlock();
        }
    }

    /**
     * Adds the given value to the current value.
     * <p>
     * The method can throw a {@link ConsistencyLostException} when the state
     * of this CRDT is not causally related to the observed timestamps. This
     * means that it cannot provide the session guarantees of RYW (read your
     * writes) and monotonic read.
     *
     * @param delta              the value to add
     * @param observedTimestamps the vector clock last observed by the client of
     *                           this counter
     * @return the current counter value with the current counter vector clock
     * @throws ConsistencyLostException if this replica cannot provide the
     *                                  session guarantees
     */
    public CRDTTimestampedLong getAndAdd(long delta, VectorClock observedTimestamps) {
        checkSessionConsistency(observedTimestamps);
        stateWriteLock.lock();
        try {
            checkNotMigrated();
            if (delta < 0) {
                return getAndSubtract(-delta, observedTimestamps);
            }
            return getAndUpdate(delta, observedTimestamps, true);
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Adds the given value to the current value.
     * <p>
     * The method can throw a {@link ConsistencyLostException} when the state
     * of this CRDT is not causally related to the observed timestamps. This
     * means that it cannot provide the session guarantees of RYW (read your
     * writes) and monotonic read.
     *
     * @param delta              the value to add
     * @param observedTimestamps the vector clock last observed by the client of
     *                           this counter
     * @return the current counter value with the current counter vector clock
     * @throws ConsistencyLostException if this replica cannot provide the
     *                                  session guarantees
     */
    public CRDTTimestampedLong addAndGet(long delta, VectorClock observedTimestamps) {
        checkSessionConsistency(observedTimestamps);
        stateWriteLock.lock();
        try {
            checkNotMigrated();
            if (delta < 0) {
                return subtractAndGet(-delta, observedTimestamps);
            }
            return updateAndGet(delta, observedTimestamps, true);
        } finally {
            stateWriteLock.unlock();
        }
    }


    /**
     * Subtracts the given value from the current value.
     * <p>
     * The method can throw a {@link ConsistencyLostException} when the state
     * of this CRDT is not causally related to the observed timestamps. This
     * means that it cannot provide the session guarantees of RYW (read your
     * writes) and monotonic read.
     *
     * @param delta              the value to add
     * @param observedTimestamps the vector clock last observed by the client of
     *                           this counter
     * @return the current counter value with the current counter vector clock
     * @throws ConsistencyLostException if this replica cannot provide the
     *                                  session guarantees
     */
    public CRDTTimestampedLong getAndSubtract(long delta, VectorClock observedTimestamps) {
        checkSessionConsistency(observedTimestamps);
        stateWriteLock.lock();
        try {
            checkNotMigrated();
            if (delta < 0) {
                return getAndAdd(-delta, observedTimestamps);
            }
            return getAndUpdate(delta, observedTimestamps, false);
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Subtracts the given value from the current value.
     * <p>
     * The method can throw a {@link ConsistencyLostException} when the state
     * of this CRDT is not causally related to the observed timestamps. This
     * means that it cannot provide the session guarantees of RYW (read your
     * writes) and monotonic read.
     *
     * @param delta              the value to subtract
     * @param observedTimestamps the vector clock last observed by the client of
     *                           this counter
     * @return the current counter value with the current counter vector clock
     * @throws ConsistencyLostException if this replica cannot provide the
     *                                  session guarantees
     */
    public CRDTTimestampedLong subtractAndGet(long delta, VectorClock observedTimestamps) {
        checkSessionConsistency(observedTimestamps);
        stateWriteLock.lock();
        try {
            checkNotMigrated();
            if (delta < 0) {
                return addAndGet(-delta, observedTimestamps);
            }
            return updateAndGet(delta, observedTimestamps, false);
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Checks if the vector clock of this PN counter state is causally before
     * the given {@code lastReadVectorClock}.
     *
     * @param lastReadVectorClock vector clock of last observed updates
     * @throws ConsistencyLostException if the CRDT state on this replica is
     *                                  not sufficiently up-to-date and does
     *                                  not reflect the last observed updates
     */
    private void checkSessionConsistency(VectorClock lastReadVectorClock) {
        if (lastReadVectorClock != null && lastReadVectorClock.isAfter(stateVectorClock)) {
            throw new ConsistencyLostException("This replica cannot provide the session guarantees for "
                    + "the PN counter since it's state is stale");
        }
    }

    /**
     * Updates the PN counter state for this replica and returns the updated value.
     * The {@code delta} parameter must be greater than or equal to 0.
     * The {@code isAddition} parameter determines if this is an addition or a
     * subtraction.
     *
     * @param delta              the delta to be applied to the current value,
     *                           must be greater than or equal to 0
     * @param observedTimestamps the last observed timestamp by the client
     * @param isAddition         if the {@code delta} should be added to or
     *                           subtracted from the current value
     * @return the PN counter value after the update
     */
    private CRDTTimestampedLong updateAndGet(long delta, VectorClock observedTimestamps, boolean isAddition) {
        if (delta < 0) {
            throw new IllegalArgumentException("Delta must be greater than or equal to 0");
        }
        final long nextTimestamp = stateVectorClock.getTimestampForReplica(localReplicaId) + 1;
        final long[] pnValues = state.containsKey(localReplicaId) ? state.get(localReplicaId) : new long[]{0, 0};
        pnValues[isAddition ? 0 : 1] += delta;
        state.put(localReplicaId, pnValues);
        stateVectorClock.setReplicaTimestamp(localReplicaId, nextTimestamp);
        return get(observedTimestamps);
    }

    /**
     * Updates the PN counter state for this replica and returns the value before
     * the update.
     * The {@code delta} parameter must be greater than or equal to 0.
     * The {@code isAddition} parameter determines if this is an addition or a
     * subtraction.
     *
     * @param delta              the delta to be applied to the current value,
     *                           must be greater than or equal to 0
     * @param observedTimestamps the last observed timestamp by the client
     * @param isAddition         if the {@code delta} should be added to or
     *                           subtracted from the current value
     * @return the PN counter value before the update
     */
    private CRDTTimestampedLong getAndUpdate(long delta, VectorClock observedTimestamps, boolean isAddition) {
        if (delta < 0) {
            throw new IllegalArgumentException("Delta must be greater than or equal to 0");
        }
        final long nextTimestamp = stateVectorClock.getTimestampForReplica(localReplicaId) + 1;
        final long[] pnValues = state.containsKey(localReplicaId) ? state.get(localReplicaId) : new long[]{0, 0};
        pnValues[isAddition ? 0 : 1] += delta;
        state.put(localReplicaId, pnValues);
        stateVectorClock.setReplicaTimestamp(localReplicaId, nextTimestamp);
        final CRDTTimestampedLong current = get(observedTimestamps);
        current.setValue(isAddition ? current.getValue() - delta : current.getValue() + delta);
        return current;
    }

    @Override
    public void merge(PNCounterImpl other) {
        stateWriteLock.lock();
        try {
            checkNotMigrated();
            for (Entry<UUID, long[]> pnCounterEntry : other.state.entrySet()) {
                final UUID replicaId = pnCounterEntry.getKey();

                final long[] pnOtherValues = pnCounterEntry.getValue();
                final long[] pnValues = state.containsKey(replicaId) ? state.get(replicaId) : new long[]{0, 0};
                pnValues[0] = Math.max(pnValues[0], pnOtherValues[0]);
                pnValues[1] = Math.max(pnValues[1], pnOtherValues[1]);
                state.put(replicaId, pnValues);
            }
            stateVectorClock.merge(other.stateVectorClock);
        } finally {
            stateWriteLock.unlock();
        }
    }

    @Override
    public VectorClock getCurrentVectorClock() {
        return new VectorClock(stateVectorClock);
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CRDTDataSerializerHook.PN_COUNTER;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        stateReadLock.lock();
        try {
            out.writeObject(stateVectorClock);
            out.writeInt(state.size());
            for (Entry<UUID, long[]> replicaState : state.entrySet()) {
                final UUID replicaID = replicaState.getKey();
                final long[] replicaCounts = replicaState.getValue();
                UUIDSerializationUtil.writeUUID(out, replicaID);
                out.writeLong(replicaCounts[0]);
                out.writeLong(replicaCounts[1]);
            }
        } finally {
            stateReadLock.unlock();
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        stateWriteLock.lock();
        try {
            stateVectorClock = in.readObject();
            final int stateSize = in.readInt();
            state = MapUtil.createHashMap(stateSize);
            for (int i = 0; i < stateSize; i++) {
                final UUID replicaID = UUIDSerializationUtil.readUUID(in);
                final long[] replicaCounts = {in.readLong(), in.readLong()};
                state.put(replicaID, replicaCounts);
            }
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Marks this CRDT as migrated. A migrated CRDT can no longer be mutated.
     * For this CRDT to be marked as migrated, the provided {@code vectorClock}
     * must be equal to the current vector clock.
     *
     * @param vectorClock a vector clock to compare the current clock to
     * @return {@code true} if this CRDT has been marked as migrated,
     * {@code false} otherwise
     */
    public boolean markMigrated(VectorClock vectorClock) {
        stateWriteLock.lock();
        try {
            if (stateVectorClock.equals(vectorClock)) {
                migrated = true;
            }
            return migrated;
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Unconditionally marks this CRDT as migrated. A migrated CRDT can no
     * longer be mutated.
     */
    public void markMigrated() {
        stateWriteLock.lock();
        try {
            migrated = true;
        } finally {
            stateWriteLock.unlock();
        }
    }

    /**
     * Throws an {@link MutationDisallowedException} if this CRDT has been migrated.
     */
    private void checkNotMigrated() {
        if (migrated) {
            throw new MutationDisallowedException("The CRDT state for the " + name
                    + " + PN counter has already been migrated and cannot be updated");
        }
    }
}
