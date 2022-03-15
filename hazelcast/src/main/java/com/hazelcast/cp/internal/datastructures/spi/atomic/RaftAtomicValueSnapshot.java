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

package com.hazelcast.cp.internal.datastructures.spi.atomic;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link RaftAtomicValueService} state for a Raft group
 */
public abstract class RaftAtomicValueSnapshot<T> implements IdentifiedDataSerializable {

    protected Map<String, T> values = Collections.emptyMap();
    protected Set<String> destroyed = Collections.emptySet();

    public RaftAtomicValueSnapshot() {
    }

    public RaftAtomicValueSnapshot(Map<String, T> values, Set<String> destroyed) {
        this.values = values;
        this.destroyed = destroyed;
    }

    public Iterable<Map.Entry<String, T>> getValues() {
        return values.entrySet();
    }

    public Set<String> getDestroyed() {
        return destroyed;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<String, T> entry : values.entrySet()) {
            out.writeString(entry.getKey());
            writeValue(out, entry.getValue());
        }

        out.writeInt(destroyed.size());
        for (String name : destroyed) {
            out.writeString(name);
        }
    }

    protected abstract void writeValue(ObjectDataOutput out, T value) throws IOException;

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        values = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readString();
            T value = readValue(in);
            values.put(name, value);
        }

        len = in.readInt();
        destroyed = new HashSet<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readString();
            destroyed.add(name);
        }
    }

    protected abstract T readValue(ObjectDataInput in) throws IOException;
}
