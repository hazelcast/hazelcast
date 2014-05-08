/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.record;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A vector clock implementation based on hashcodes of the Hazelcast members UUID to solve conflicts on
 * replication updates
 */
public final class VectorClock
        implements IdentifiedDataSerializable {

    final Map<Member, AtomicInteger> clocks;

    public VectorClock() {
        clocks = new ConcurrentHashMap<Member, AtomicInteger>();
    }

    void incrementClock(Member localMember) {
        final AtomicInteger clock = clocks.get(localMember);
        if (clock != null) {
            clock.incrementAndGet();
        } else {
            clocks.put(localMember, new AtomicInteger(1));
        }
    }

    void applyVector(VectorClock update) {
        for (Member m : update.clocks.keySet()) {
            final AtomicInteger currentClock = clocks.get(m);
            final AtomicInteger updateClock = update.clocks.get(m);
            if (smaller(currentClock, updateClock)) {
                clocks.put(m, new AtomicInteger(updateClock.get()));
            }
        }
    }

    @Override
    public void writeData(ObjectDataOutput dataOutput)
            throws IOException {
        dataOutput.writeInt(clocks.size());
        for (Entry<Member, AtomicInteger> entry : clocks.entrySet()) {
            entry.getKey().writeData(dataOutput);
            dataOutput.writeInt(entry.getValue().get());
        }
    }

    @Override
    public void readData(ObjectDataInput dataInput)
            throws IOException {
        int size = dataInput.readInt();
        for (int i = 0; i < size; i++) {
            Member m = new MemberImpl();
            m.readData(dataInput);
            int clock = dataInput.readInt();
            clocks.put(m, new AtomicInteger(clock));
        }
    }

    @Override
    public int getId() {
        return ReplicatedMapDataSerializerHook.VECTOR;
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapDataSerializerHook.F_ID;
    }

    @Override
    public String toString() {
        return "Vector{" + "clocks=" + clocks + '}';
    }

    private boolean smaller(AtomicInteger int1, AtomicInteger int2) {
        int i1 = int1 == null ? 0 : int1.get();
        int i2 = int2 == null ? 0 : int2.get();
        return i1 < i2;
    }

    static VectorClock copyVector(VectorClock vectorClock) {
        VectorClock copy = new VectorClock();
        Map<Member, AtomicInteger> clocks = copy.clocks;
        for (Entry<Member, AtomicInteger> entry : vectorClock.clocks.entrySet()) {
            MemberImpl member = new MemberImpl((MemberImpl) entry.getKey());
            AtomicInteger value = new AtomicInteger(entry.getValue().intValue());
            clocks.put(member, value);
        }
        return copy;
    }

    static boolean happenedBefore(VectorClock x, VectorClock y) {
        Set<Member> members = new HashSet<Member>(x.clocks.keySet());
        members.addAll(y.clocks.keySet());

        boolean hasLesser = false;
        for (Member m : members) {
            int xi = x.clocks.get(m) != null ? x.clocks.get(m).get() : 0;
            int yi = y.clocks.get(m) != null ? y.clocks.get(m).get() : 0;
            if (xi > yi) {
                return false;
            }
            if (xi < yi) {
                hasLesser = true;
            }
        }
        return hasLesser;
    }
}
