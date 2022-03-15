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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.internal.util.BiTuple;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static com.hazelcast.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.internal.util.UUIDSerializationUtil.writeUUID;
import static java.lang.Math.max;

/**
 * State-machine implementation of the Raft-based count down latch
 */
public class CountDownLatch extends BlockingResource<AwaitInvocationKey> implements IdentifiedDataSerializable {

    private volatile int round;
    private volatile int countDownFrom;
    private transient volatile int remaining;
    private final Set<UUID> countDownUids = new HashSet<>();

    CountDownLatch() {
    }

    CountDownLatch(CPGroupId groupId, String name) {
        super(groupId, name);
    }

    /**
     * Reduces remaining count of the latch.
     * If the expected round is smaller than the current round, it is either
     * a retry or a countDown() request sent before re-initialization
     * of the latch. In this case, this count down request is ignored.
     */
    BiTuple<Integer, Collection<AwaitInvocationKey>> countDown(UUID invocationUuid, int expectedRound) {
        if (expectedRound > round) {
            throw new IllegalStateException("Could not could count down the latch because expected round: " + expectedRound
                    + " is bigger than the actual round: " + round + ". This can happen when CP Subsystem is used in the unsafe "
                    + "mode if data loss occurs after the count reaches to zero and the latch is reinitialized.");
        }

        if (expectedRound < round) {
            Collection<AwaitInvocationKey> c = Collections.emptyList();
            return BiTuple.of(0, c);
        }

        countDownUids.add(invocationUuid);
        int remaining = updateRemainingCount();
        if (remaining > 0) {
            Collection<AwaitInvocationKey> c = Collections.emptyList();
            return BiTuple.of(remaining, c);
        }

        Collection<AwaitInvocationKey> w = getAllWaitKeys();
        clearWaitKeys();

        return BiTuple.of(0, w);
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "'round' field is updated only by a single thread.")
    @SuppressWarnings("NonAtomicOperationOnVolatileField")
    boolean trySetCount(int count) {
        checkTrue(count > 0, "cannot set non-positive count: " + count);

        if (getRemainingCount() > 0) {
            return false;
        }

        countDownFrom = count;
        round++;
        countDownUids.clear();
        updateRemainingCount();

        return true;
    }

    boolean await(AwaitInvocationKey key, boolean wait) {
        boolean success = (getRemainingCount() == 0);
        if (!success && wait) {
            addWaitKey(key.invocationUid(), key);
        }

        return success;
    }

    int getRound() {
        return round;
    }

    public int getCount() {
        return countDownFrom;
    }

    int updateRemainingCount() {
        int rem = max(0, countDownFrom - countDownUids.size());
        remaining = rem;
        return rem;
    }

    int getRemainingCount() {
        return remaining;
    }

    CountDownLatch cloneForSnapshot() {
        CountDownLatch clone = new CountDownLatch();
        cloneForSnapshot(clone);
        clone.round = this.round;
        clone.countDownFrom = this.countDownFrom;
        clone.countDownUids.addAll(this.countDownUids);

        return clone;
    }

    @Override
    protected void onSessionClose(long sessionId, Map<Long, Object> responses) {
    }

    @Override
    protected Collection<Long> getActivelyAttachedSessions() {
        return Collections.emptyList();
    }

    @Override
    public int getFactoryId() {
        return CountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CountDownLatchDataSerializerHook.COUNT_DOWN_LATCH;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);

        out.writeInt(round);
        out.writeInt(countDownFrom);
        out.writeInt(countDownUids.size());
        for (UUID uid : countDownUids) {
            writeUUID(out, uid);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        round = in.readInt();
        countDownFrom = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            countDownUids.add(readUUID(in));
        }
        updateRemainingCount();
    }

    @Override
    public String toString() {
        return "CountDownLatch{" + internalToString() + ", round=" + round
                + ", countDownFrom=" + countDownFrom + ", countDownUids=" + countDownUids + '}';
    }
}
