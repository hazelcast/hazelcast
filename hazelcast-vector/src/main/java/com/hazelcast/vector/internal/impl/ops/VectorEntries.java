/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.vector.internal.impl.ops;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.CollectionUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.vector.impl.DataVectorDocument;
import com.hazelcast.vector.internal.impl.VectorCollectionSerializerHook;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * {@link VectorEntries} is a collection of {@link Data} instances for keys and values
 * of a {@link com.hazelcast.vector.VectorCollection}.
 */
public final class VectorEntries implements IdentifiedDataSerializable {

    private List<Data> keys;
    private List<DataVectorDocument> documents;

    public VectorEntries() {
    }

    public VectorEntries(int initialSize) {
        keys = new ArrayList<>(initialSize);
        documents = new ArrayList<>(initialSize);
    }

    public VectorEntries(Collection<Map.Entry<Data, DataVectorDocument>> entries) {
        int initialSize = entries.size();
        keys = new ArrayList<>(initialSize);
        documents = new ArrayList<>(initialSize);
        for (Map.Entry<Data, DataVectorDocument> entry : entries) {
            keys.add(entry.getKey());
            documents.add(entry.getValue());
        }
    }

    public void add(Data key, DataVectorDocument document) {
        ensureEntriesCreated();
        keys.add(key);
        documents.add(document);
    }

    public Data getKey(int index) {
        return keys.get(index);
    }

    public DataVectorDocument getDocument(int index) {
        return documents.get(index);
    }

    public int size() {
        return (keys == null ? 0 : keys.size());
    }

    public boolean isEmpty() {
        return CollectionUtil.isEmpty(keys);
    }

    public void clear() {
        if (keys != null) {
            keys.clear();
            documents.clear();
        }
    }

    private void ensureEntriesCreated() {
        if (keys == null) {
            keys = new ArrayList<>();
            documents = new ArrayList<>();
        }
    }

    @Override
    public int getFactoryId() {
        return VectorCollectionSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return VectorCollectionSerializerHook.ENTRIES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        int size = size();
        out.writeInt(size);
        for (int i = 0; i < size; i++) {
            IOUtil.writeData(out, keys.get(i));
            out.writeObject(documents.get(i));
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        keys = new ArrayList<>(size);
        documents = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            keys.add(IOUtil.readData(in));
            documents.add(in.readObject());
        }
    }
}
