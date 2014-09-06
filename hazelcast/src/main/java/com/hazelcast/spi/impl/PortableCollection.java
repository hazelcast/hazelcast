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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public final class PortableCollection implements Portable {

    private Collection<Data> collection;

    public PortableCollection() {
    }

    public PortableCollection(Collection<Data> collection) {
        this.collection = collection;
    }

    public Collection<Data> getCollection() {
        return collection;
    }

    @Override
    public int getFactoryId() {
        return SpiPortableHook.ID;
    }

    @Override
    public int getClassId() {
        return SpiPortableHook.COLLECTION;
    }

    @Override
    public void writePortable(PortableWriter writer) throws IOException {
        writer.writeBoolean("l", collection instanceof List);
        if (collection == null) {
            writer.writeInt("s", -1);
            return;
        }
        writer.writeInt("s", collection.size());
        final ObjectDataOutput out = writer.getRawDataOutput();
        for (Data data : collection) {
            data.writeData(out);
        }
    }

    @Override
    public void readPortable(PortableReader reader) throws IOException {
        boolean list = reader.readBoolean("l");
        int size = reader.readInt("s");
        if (size == -1) {
            return;
        }
        if (list) {
            collection = new ArrayList<Data>(size);
        } else {
            collection = new HashSet<Data>(size);
        }
        final ObjectDataInput in = reader.getRawDataInput();
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            collection.add(data);
        }
    }
}
