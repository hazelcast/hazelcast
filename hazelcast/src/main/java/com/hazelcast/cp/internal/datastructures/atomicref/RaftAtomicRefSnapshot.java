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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link RaftAtomicRefService} state for a Raft group
 */
public class RaftAtomicRefSnapshot implements IdentifiedDataSerializable {

    private Map<String, Data> refs = Collections.emptyMap();
    private Set<String> destroyed = Collections.emptySet();

    public RaftAtomicRefSnapshot() {
    }

    public RaftAtomicRefSnapshot(Map<String, Data> refs, Set<String> destroyed) {
        this.refs = refs;
        this.destroyed = destroyed;
    }

    public Iterable<Map.Entry<String, Data>> getRefs() {
        return refs.entrySet();
    }

    public Set<String> getDestroyed() {
        return destroyed;
    }

    @Override
    public int getFactoryId() {
        return RaftAtomicReferenceDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return RaftAtomicReferenceDataSerializerHook.SNAPSHOT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(refs.size());
        for (Map.Entry<String, Data> entry : refs.entrySet()) {
            out.writeUTF(entry.getKey());
            out.writeData(entry.getValue());
        }

        out.writeInt(destroyed.size());
        for (String name : destroyed) {
            out.writeUTF(name);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        refs = new HashMap<String, Data>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readUTF();
            Data value = in.readData();
            refs.put(name, value);
        }

        len = in.readInt();
        destroyed = new HashSet<String>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readUTF();
            destroyed.add(name);
        }
    }

    @Override
    public String toString() {
        return "RaftAtomicRefSnapshot{" + "refs=" + refs + ", destroyed=" + destroyed + '}';
    }
}
