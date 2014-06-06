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

import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public final class SerializableCollection implements IdentifiedDataSerializable, Iterable<Data> {

    private Collection<Data> collection;

    public SerializableCollection() {
    }

    public SerializableCollection(Collection<Data> collection) {
        this.collection = collection;
    }

    public SerializableCollection(Data... dataArray) {
        this.collection = new ArrayList<Data>(dataArray.length);
        Collections.addAll(collection, dataArray);
    }

    public Collection<Data> getCollection() {
        return collection;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (collection == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(collection.size());
        for (Data data : collection) {
            data.writeData(out);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size == -1) {
            return;
        }
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
    public int getId() {
        return SpiDataSerializerHook.COLLECTION;
    }

    @Override
    public Iterator<Data> iterator() {
        final Iterator<Data> iterator = collection == null ? null : collection.iterator();
        return new Iterator<Data>() {
            @Override
            public boolean hasNext() {
                return iterator != null && iterator.hasNext();
            }

            @Override
            public Data next() {
                return iterator.next();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };
    }

    public int size() {
        return collection == null ? 0 : collection.size();
    }
}
