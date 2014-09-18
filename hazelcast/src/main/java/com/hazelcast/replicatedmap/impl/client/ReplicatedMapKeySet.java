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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Class to implement a replicated map keySet result
 */
public class ReplicatedMapKeySet
        implements Portable {

    private Set keySet;

    ReplicatedMapKeySet() {
    }

    public ReplicatedMapKeySet(Set keySet) {
        this.keySet = keySet;
    }

    public Set getKeySet() {
        return keySet;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeInt("size", keySet.size());
        ObjectDataOutput out = writer.getRawDataOutput();
        for (Object key : keySet) {
            out.writeObject(key);
        }
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        int size = reader.readInt("size");
        ObjectDataInput in = reader.getRawDataInput();
        keySet = new HashSet(size);
        for (int i = 0; i < size; i++) {
            keySet.add(in.readObject());
        }
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.MAP_KEY_SET;
    }

}
