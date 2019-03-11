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

package com.hazelcast.cp.internal.datastructures.countdownlatch;

import com.hazelcast.cp.CPGroupId;
import com.hazelcast.cp.internal.datastructures.spi.blocking.BlockingResource;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.readUUID;
import static com.hazelcast.cp.internal.util.UUIDSerializationUtil.writeUUID;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * State-machine implementation of the Raft-based count down latch
 */
public class RaftCountDownLatch extends BlockingResource<AwaitInvocationKey> implements IdentifiedDataSerializable {

    private int round;
    private int countDownFrom;
    private final Set<UUID> countDownUids = new HashSet<UUID>();

    RaftCountDownLatch() {
    }

    RaftCountDownLatch(CPGroupId groupId, String name) {
        super(groupId, name);
    }

    /**
     * Reduces remaining count of the latch.
     * If the expected round is smaller than the current round, it is either
     * a retry or a countDown() request sent before re-initialization
     * of the latch. In this case, this count down request is ignored.
     */
    Tuple2<Integer, Collection<AwaitInvocationKey>> countDown(UUID invocationUuid, int expectedRound) {
        if (expectedRound > round) {
            throw new IllegalArgumentException("expected round: " + expectedRound + ", actual round: " + round);
        }

        if (expectedRound < round) {
            Collection<AwaitInvocationKey> c = Collections.emptyList();
            return Tuple2.of(0, c);
        }

        countDownUids.add(invocationUuid);
        int remaining = getRemainingCount();
        if (remaining > 0) {
            Collection<AwaitInvocationKey> c = Collections.emptyList();
            return Tuple2.of(remaining, c);
        }

        Collection<AwaitInvocationKey> w = getAllWaitKeys();
        clearWaitKeys();

        return Tuple2.of(0, w);
    }

    boolean trySetCount(int count) {
        if (getRemainingCount() > 0) {
            return false;
        }

        checkTrue(count > 0, "cannot set non-positive count: " + count);

        countDownFrom = count;
        round++;
        countDownUids.clear();

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

    int getRemainingCount() {
        return max(0, countDownFrom - countDownUids.size());
    }

    RaftCountDownLatch cloneForSnapshot() {
        RaftCountDownLatch clone = new RaftCountDownLatch();
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
        return RaftCountDownLatchDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftCountDownLatchDataSerializerHook.COUNT_DOWN_LATCH;
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
    }

    @Override
    public String toString() {
        return "RaftCountDownLatch{" + internalToString() + ", round=" + round
                + ", countDownFrom=" + countDownFrom + ", countDownUids=" + countDownUids + '}';
    }
}
