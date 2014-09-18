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

package com.hazelcast.map;

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class MapEntrySet implements IdentifiedDataSerializable {

    private Set<Map.Entry<Data, Data>> entrySet;

    public MapEntrySet() {
        entrySet = new HashSet<Map.Entry<Data, Data>>();
    }

    public MapEntrySet(Set<Map.Entry<Data, Data>> entrySet) {
        this.entrySet = entrySet;
    }

    public Set<Map.Entry<Data, Data>> getEntrySet() {
        return entrySet;
    }

    public void add(Map.Entry<Data, Data> entry) {
        entrySet.add(entry);
    }

    public void add(Data key, Data value) {
        entrySet.add(new AbstractMap.SimpleImmutableEntry<Data, Data>(key, value));
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        int size = entrySet.size();
        out.writeInt(size);
        for (Map.Entry<Data, Data> o : entrySet) {
            o.getKey().writeData(out);
            o.getValue().writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            Map.Entry entry = new AbstractMap.SimpleImmutableEntry<Data, Data>(IOUtil.readData(in), IOUtil.readData(in));
            entrySet.add(entry);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.ENTRY_SET;
    }

}
