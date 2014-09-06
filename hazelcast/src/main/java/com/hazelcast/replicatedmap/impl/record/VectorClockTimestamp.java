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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.Member;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.replicatedmap.impl.operation.ReplicatedMapDataSerializerHook;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A vector clock implementation based on hashcodes of the Hazelcast members UUID to solve conflicts on
 * replication updates
 */
public final class VectorClockTimestamp
        implements IdentifiedDataSerializable {

    private Map<Member, Integer> clocks;

    public VectorClockTimestamp() {
        this.clocks = Collections.emptyMap();
    }

    private VectorClockTimestamp(Map<Member, Integer> clocks) {
        this.clocks = Collections.unmodifiableMap(clocks);
    }

    VectorClockTimestamp incrementClock0(Member localMember) {
        Map<Member, Integer> copy = new HashMap<Member, Integer>(clocks);
        Integer clock = copy.get(localMember);
        if (clock == null) {
            clock = 0;
        }

        copy.put(localMember, ++clock);
        return new VectorClockTimestamp(copy);
    }

    VectorClockTimestamp applyVector0(VectorClockTimestamp update) {
        Map<Member, Integer> copy = new HashMap<Member, Integer>(clocks);
        for (Member m : update.clocks.keySet()) {
            final Integer currentClock = copy.get(m);
            final Integer updateClock = update.clocks.get(m);
            if (smaller(currentClock, updateClock)) {
                copy.put(m, updateClock);
            }
        }
        return new VectorClockTimestamp(copy);
    }

    @Override
    public void writeData(ObjectDataOutput dataOutput)
            throws IOException {

        Map<Member, Integer> clocks = this.clocks;
        dataOutput.writeInt(clocks.size());
        for (Entry<Member, Integer> entry : clocks.entrySet()) {
            entry.getKey().writeData(dataOutput);
            dataOutput.writeInt(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput dataInput)
            throws IOException {

        int size = dataInput.readInt();
        Map<Member, Integer> data = new HashMap<Member, Integer>();
        for (int i = 0; i < size; i++) {
            Member m = new MemberImpl();
            m.readData(dataInput);
            int clock = dataInput.readInt();
            data.put(m, clock);
        }
        this.clocks = Collections.unmodifiableMap(data);
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

    private boolean smaller(Integer int1, Integer int2) {
        int i1 = int1 == null ? 0 : int1;
        int i2 = int2 == null ? 0 : int2;
        return i1 < i2;
    }

    static VectorClockTimestamp copyVector(VectorClockTimestamp vectorClockTimestamp) {
        Map<Member, Integer> clocks = new HashMap<Member, Integer>();
        for (Entry<Member, Integer> entry : vectorClockTimestamp.clocks.entrySet()) {
            MemberImpl member = new MemberImpl((MemberImpl) entry.getKey());
            Integer value = entry.getValue();
            clocks.put(member, value);
        }
        return new VectorClockTimestamp(clocks);
    }

    static boolean happenedBefore(VectorClockTimestamp x, VectorClockTimestamp y) {
        Set<Member> members = new HashSet<Member>(x.clocks.keySet());
        members.addAll(y.clocks.keySet());

        boolean hasLesser = false;
        for (Member m : members) {
            int xi = x.clocks.get(m) != null ? x.clocks.get(m) : 0;
            int yi = y.clocks.get(m) != null ? y.clocks.get(m) : 0;
            if (xi > yi) {
                return false;
            }
            if (xi < yi) {
                hasLesser = true;
            }
        }
        return hasLesser;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        VectorClockTimestamp that = (VectorClockTimestamp) o;
        if (clocks != null ? !clocks.equals(that.clocks) : that.clocks != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return clocks != null ? clocks.hashCode() : 0;
    }
}
