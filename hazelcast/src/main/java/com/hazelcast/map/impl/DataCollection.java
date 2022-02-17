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

package com.hazelcast.map.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

public class DataCollection implements IdentifiedDataSerializable {

    private Collection<Data> values;

    public DataCollection(Collection<Data> values) {
        this.values = values;
    }

    public DataCollection() {
    }

    public Collection<Data> getCollection() {
        return values;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = values.size();
        out.writeInt(size);
        for (Data o : values) {
            IOUtil.writeData(out, o);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        values = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Data data = IOUtil.readData(in);
            values.add(data);
        }
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.DATA_COLLECTION;
    }
}
