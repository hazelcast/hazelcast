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

package com.hazelcast.cache.impl;

import com.hazelcast.internal.iteration.IterationPointer;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Container class for a collection of entries along with pointers defining
 * the iteration state from which new keys can be fetched.
 * This class is usually used when iterating cache entries.
 *
 * @see CacheProxy#iterator
 */
public class CacheEntriesWithCursor implements IdentifiedDataSerializable {
    private List<Map.Entry<Data, Data>> entries;
    private IterationPointer[] pointers;

    public CacheEntriesWithCursor() {
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2", justification = "This is an internal class")
    public CacheEntriesWithCursor(List<Map.Entry<Data, Data>> entries, IterationPointer[] pointers) {
        this.entries = entries;
        this.pointers = pointers;
    }

    /**
     * Returns the iteration pointers representing the current iteration state.
     */
    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "This is an internal class")
    public IterationPointer[] getPointers() {
        return pointers;
    }

    public List<Map.Entry<Data, Data>> getEntries() {
        return entries;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CacheDataSerializerHook.ENTRY_ITERATION_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(pointers.length);
        for (IterationPointer pointer : pointers) {
            out.writeInt(pointer.getIndex());
            out.writeInt(pointer.getSize());
        }
        int size = entries.size();
        out.writeInt(size);
        for (Map.Entry<Data, Data> entry : entries) {
            IOUtil.writeData(out, entry.getKey());
            IOUtil.writeData(out, entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int pointersCount = in.readInt();
        pointers = new IterationPointer[pointersCount];
        for (int i = 0; i < pointersCount; i++) {
            pointers[i] = new IterationPointer(in.readInt(), in.readInt());
        }
        int size = in.readInt();
        entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Data key = IOUtil.readData(in);
            Data value = IOUtil.readData(in);
            entries.add(new AbstractMap.SimpleEntry<Data, Data>(key, value));
        }
    }

    @Override
    public String toString() {
        return "CacheEntryIteratorResult";
    }

    public int getCount() {
        return entries != null ? entries.size() : 0;
    }
}
