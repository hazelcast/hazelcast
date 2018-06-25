/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.raft.service.countdownlatch;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.raft.RaftGroupId;
import com.hazelcast.raft.impl.util.Tuple2;
import com.hazelcast.raft.service.blocking.BlockingResource;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.Math.max;

/**
 * State-machine implementation of the Raft-based count down latch
 */
public class RaftCountDownLatch extends BlockingResource<CountDownLatchInvocationKey> implements IdentifiedDataSerializable {

    private int round;
    private int countDownFrom;
    private final Set<UUID> countDownUids = new HashSet<UUID>();

    RaftCountDownLatch() {
    }

    RaftCountDownLatch(RaftGroupId groupId, String name) {
        super(groupId, name);
    }

    /**
     * Reduces remaining count of the latch.
     * If the expected round is smaller than the current round, it is either a retry or a countDown() request
     * sent before re-initialization of the latch. In this case, this count down request is ignored.
     */
    Tuple2<Integer, Collection<CountDownLatchInvocationKey>> countDown(int expectedRound, UUID invocationUuid) {
        if (expectedRound > round) {
            throw new IllegalArgumentException("expected round: " + expectedRound + ", actual round: " + round);
        }

        if (expectedRound < round) {
            Collection<CountDownLatchInvocationKey> c = Collections.emptyList();
            return Tuple2.of(0, c);
        }

        countDownUids.add(invocationUuid);
        int remaining = getRemainingCount();
        if (remaining > 0) {
            Collection<CountDownLatchInvocationKey> c = Collections.emptyList();
            return Tuple2.of(remaining, c);
        }

        Collection<CountDownLatchInvocationKey> w = new ArrayList<CountDownLatchInvocationKey>(waitKeys);
        waitKeys.clear();

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

    boolean await(long commitIndex, boolean wait) {
        boolean success = (getRemainingCount() == 0);
        if (!success && wait) {
            waitKeys.add(new CountDownLatchInvocationKey(name, commitIndex));
        }

        return success;
    }

    int getRound() {
        return round;
    }

    int getRemainingCount() {
        return max(0, countDownFrom - countDownUids.size());
    }

    @Override
    protected void onSessionClose(long sessionId, Long2ObjectHashMap<Object> responses) {
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
            out.writeLong(uid.getLeastSignificantBits());
            out.writeLong(uid.getMostSignificantBits());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);

        round = in.readInt();
        countDownFrom = in.readInt();
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            long least = in.readLong();
            long most = in.readLong();
            UUID uid = new UUID(most, least);
            countDownUids.add(uid);
        }
    }

    @Override
    public String toString() {
        return "RaftCountDownLatch{" + "groupId=" + groupId + ", name='" + name + '\'' + ", round=" + round
                + ", countDownFrom=" + countDownFrom + ", countDownUids=" + countDownUids + ", waitKeys=" + waitKeys + '}';
    }
}
