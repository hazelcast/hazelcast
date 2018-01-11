/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
 * <p>Response data object returned by {@link com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation }.</p>
 * This result wrapper is used in {@link AbstractClusterWideIterator}'s subclasses to return a collection of keys
 * and the last tableIndex processed.
 *
 * @see AbstractClusterWideIterator
 * @see com.hazelcast.cache.impl.operation.CacheKeyIteratorOperation
 */
public class CacheKeyIterationResult implements IdentifiedDataSerializable {

    private int tableIndex;
    private List<Data> keys;

    public CacheKeyIterationResult() {
    }

    public CacheKeyIterationResult(List<Data> keys, int tableIndex) {
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
            out.writeData(o);
        }

    }

    @Override
    public void readData(ObjectDataInput in)
            throws IOException {
        tableIndex = in.readInt();
        int size = in.readInt();
        keys = new ArrayList<Data>(size);
        for (int i = 0; i < size; i++) {
            Data data = in.readData();
            keys.add(data);
        }
    }

    @Override
    public String toString() {
        return "CacheKeyIteratorResult{tableIndex=" + tableIndex + '}';
    }

    public int getCount() {
        return keys != null ? keys.size() : 0;
    }

    public Data getKey(int index) {
        return keys != null ? keys.get(index) : null;
    }
}
