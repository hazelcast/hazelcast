/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.cpmap;

import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
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
 * Snapshot of a {@link CPMapService} state for a Raft group
 */
public class CPMapSnapshot implements IdentifiedDataSerializable {

    protected Map<String, Map<Object,Object>> values = Collections.emptyMap();
    protected Set<String> destroyed = Collections.emptySet();

    public CPMapSnapshot() {
    }

    public CPMapSnapshot(Map<String, Map<Object,Object>> maps, Set<String> destroyed) {
        this.values = values;
        this.destroyed = destroyed;
    }

    public Iterable<Map.Entry<String, Map<Object,Object>>> getValues() {
        return values.entrySet();
    }

    public Set<String> getDestroyed() {
        return destroyed;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(values.size());
        for (Map.Entry<String, Map<Object,Object>> entry : values.entrySet()) {
            out.writeString(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(destroyed.size());
        for (String name : destroyed) {
            out.writeString(name);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int len = in.readInt();
        values = new HashMap<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readString();
            Map<Object,Object> value = (Map)in.readObject();
            values.put(name, value);
        }

        len = in.readInt();
        destroyed = new HashSet<>(len);
        for (int i = 0; i < len; i++) {
            String name = in.readString();
            destroyed.add(name);
        }
    }

    @Override
    public int getFactoryId() {
        return CPMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CPMapDataSerializerHook.SNAPSHOT;
    }

    @Override
    public String toString() {
        return "CPMapSNapshot{" + "values=" + values + ", destroyed=" + destroyed + '}';
    }
}
