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

package com.hazelcast.cache.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Result of the {@link com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation }
 */
public class CacheKeyIteratorResult
        implements IdentifiedDataSerializable {

    private int tableIndex;
    private List<Data> keys;

    public CacheKeyIteratorResult() {
    }

    public CacheKeyIteratorResult(List<Data> keys, int tableIndex) {
        this.keys = keys;
        this.tableIndex = tableIndex;
    }

    public int getTableIndex() {
        return tableIndex;
    }

    public List<Data> getKeys() {
        return keys;
    }

    @Override
    public int getFactoryId() {
        return CacheDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return CacheDataSerializerHook.KEY_ITERATION_RESULT;
    }

    @Override
    public void writeData(ObjectDataOutput out)
            throws IOException {
        out.writeInt(tableIndex);
        int size = keys.size();
        out.writeInt(size);
        for (Data o : keys) {
            o.writeData(out);
        }

    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        tableIndex = in.readInt();
        int size = in.readInt();
        keys = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = new Data();
            data.readData(in);
            keys.add(data);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CacheKeyIteratorResult{");
        sb.append(", tableIndex=").append(tableIndex);
        sb.append('}');
        return sb.toString();
    }

    public int getCount() {
        return keys != null ? keys.size() : 0;
    }

    public Data getKey(int index) {
        return keys != null ? keys.get(index) : null;
    }
}
