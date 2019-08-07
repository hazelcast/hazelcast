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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link RaftAtomicLongService} state for a Raft group
 */
public class RaftAtomicLongSnapshot implements IdentifiedDataSerializable {

    private Map<String, Long> longs = Collections.emptyMap();
    private Set<String> destroyed = Collections.emptySet();

    public RaftAtomicLongSnapshot() {
    }

    public RaftAtomicLongSnapshot(Map<String, Long> longs, Set<String> destroyed) {
        this.longs = longs;
        this.destroyed = destroyed;
    }

    public Iterable<Map.Entry<String, Long>> getLongs() {
        return longs.entrySet();
    }

    public Set<String> getDestroyed() {
        return destroyed;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(longs.size());
        for (Map.Entry<String, Long> entry : longs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeLong(entry.getValue());
        }

        out.writeInt(destroyed.size());
        for (String name : destroyed) {
            out.writeUTF(name);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        longs = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readUTF();
            long value = in.readLong();
            longs.put(name, value);
        }

        len = in.readInt();
        destroyed = new HashSet<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readUTF();
            destroyed.add(name);
        }
    }

    @Override
    public int getFactoryId() {
        return RaftAtomicLongDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftAtomicLongDataSerializerHook.SNAPSHOT;
    }

    @Override
    public String toString() {
        return "RaftAtomicLongSnapshot{" + "longs=" + longs + ", destroyed=" + destroyed + '}';
    }
}
