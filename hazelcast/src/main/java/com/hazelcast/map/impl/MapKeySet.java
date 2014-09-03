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

package com.hazelcast.map.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class MapKeySet implements IdentifiedDataSerializable {

    Set<Data> keySet;


    public MapKeySet(Set<Data> keySet) {
        this.keySet = keySet;
    }

    public MapKeySet() {
    }

    public Set<Data> getKeySet() {
        return keySet;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        int size = keySet.size();
        out.writeInt(size);
        for (Data o : keySet) {
            o.writeData(out);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        keySet = new HashSet<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            keySet.add(data);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.KEY_SET;
    }
}
