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

package com.hazelcast.query.impl;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 *  Multiple result set for Predicates
 */

public class QueryResultEntryImpl implements IdentifiedDataSerializable, QueryResultEntry {
    private Data indexKey;
    private Data keyData;
    private Data valueData;

    public QueryResultEntryImpl() {
    }

    public QueryResultEntryImpl(Data keyData, Data indexKey, Data valueData) {
        this.keyData = keyData;
        this.indexKey = indexKey;
        this.valueData = valueData;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeNullableData(out, getIndexKey());
        IOUtil.writeNullableData(out, getKeyData());
        IOUtil.writeNullableData(out, getValueData());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        indexKey = IOUtil.readNullableData(in);
        keyData = IOUtil.readNullableData(in);
        valueData = IOUtil.readNullableData(in);
    }

    @Override
    public Data getKeyData() {
        return keyData;
    }

    @Override
    public Data getValueData() {
        return valueData;
    }

    @Override
    public Data getIndexKey() {
        return indexKey;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.QUERY_RESULT_ENTRY;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryResultEntryImpl that = (QueryResultEntryImpl) o;

        if (indexKey != null ? !indexKey.equals(that.indexKey) : that.indexKey != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return indexKey != null ? indexKey.hashCode() : 0;
    }
}
