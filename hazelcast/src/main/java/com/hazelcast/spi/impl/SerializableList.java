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

package com.hazelcast.spi.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.internal.util.UnmodifiableIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SerializableList implements IdentifiedDataSerializable, Iterable<Data> {

    private List<Data> collection;

    public SerializableList() {
    }

    public SerializableList(List<Data> collection) {
        this.collection = collection;
    }

    public List<Data> getCollection() {
        return collection;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(collection.size());
        for (Data data : collection) {
            IOUtil.writeData(out, data);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        collection = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            collection.add(IOUtil.readData(in));
        }
    }

    @Override
    public int getFactoryId() {
        return SpiDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return SpiDataSerializerHook.COLLECTION;
    }

    @Override
    public Iterator<Data> iterator() {
        final Iterator<Data> iterator = collection.iterator();
        return new UnmodifiableIterator<Data>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Data next() {
                return iterator.next();
            }
        };
    }

    public int size() {
        return collection.size();
    }
}
