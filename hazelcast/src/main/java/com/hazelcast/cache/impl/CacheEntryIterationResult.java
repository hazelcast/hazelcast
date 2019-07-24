/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * <p>Response data object returned by {@link com.hazelcast.cache.impl.operation.CacheEntryIteratorOperation }.</p>
 * This result wrapper is used in {@link AbstractClusterWideIterator}'s subclasses to return a collection of entries
 * and the last tableIndex processed.
 *
 * @see AbstractClusterWideIterator
 * @see com.hazelcast.cache.impl.operation.CacheEntryIteratorOperation
 */
public class CacheEntryIterationResult implements IdentifiedDataSerializable {

    private int tableIndex;
    private List<Map.Entry<Data, Data>> entries;

    public CacheEntryIterationResult() {
    }

    public CacheEntryIterationResult(List<Map.Entry<Data, Data>> entries, int tableIndex) {
        this.entries = entries;
        this.tableIndex = tableIndex;
    }

    public int getTableIndex() {
        return tableIndex;
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
        out.writeInt(tableIndex);
        int size = entries.size();
        out.writeInt(size);
        for (Map.Entry<Data, Data> entry : entries) {
            out.writeData(entry.getKey());
            out.writeData(entry.getValue());
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        tableIndex = in.readInt();
        int size = in.readInt();
        entries = new ArrayList<Map.Entry<Data, Data>>(size);
        for (int i = 0; i < size; i++) {
            Data key = in.readData();
            Data value = in.readData();
            entries.add(new AbstractMap.SimpleEntry<Data, Data>(key, value));
        }
    }

    @Override
    public String toString() {
        return "CacheEntryIteratorResult{tableIndex=" + tableIndex + '}';
    }

    public int getCount() {
        return entries != null ? entries.size() : 0;
    }
}
