/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.iterator;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MapKeysWithCursor implements IdentifiedDataSerializable {

    private int nextTableIndexToReadFrom;
    private List<Data> keys;

    public MapKeysWithCursor() {
    }

    public MapKeysWithCursor(List<Data> keys, int nextTableIndexToReadFrom) {
        this.keys = keys;
        this.nextTableIndexToReadFrom = nextTableIndexToReadFrom;
    }

    public int getNextTableIndexToReadFrom() {
        return nextTableIndexToReadFrom;
    }

    public List<Data> getKeys() {
        return keys;
    }

    public int getCount() {
        return keys != null ? keys.size() : 0;
    }

    public Data getKey(int index) {
        return keys != null ? keys.get(index) : null;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nextTableIndexToReadFrom);
        int size = keys.size();
        out.writeInt(size);
        for (Data o : keys) {
            out.writeData(o);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nextTableIndexToReadFrom = in.readInt();
        int size = in.readInt();
        keys = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.KEYS_WITH_CURSOR;
    }
}
