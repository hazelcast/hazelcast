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

import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class QueryResultEntryImpl implements IdentifiedDataSerializable, QueryResultEntry {
    Data indexKey;
    Data keyData;
    Data valueData;

    public QueryResultEntryImpl() {
    }

    public QueryResultEntryImpl(Data keyData, Data indexKey, Data valueData) {
        this.keyData = keyData;
        this.indexKey = indexKey;
        this.valueData = valueData;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeNullableData(out, getIndexKey());
        IOUtil.writeNullableData(out, getKeyData());
        IOUtil.writeNullableData(out, getValueData());
    }

    public void readData(ObjectDataInput in) throws IOException {
        indexKey = IOUtil.readNullableData(in);
        keyData = IOUtil.readNullableData(in);
        valueData = IOUtil.readNullableData(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryResultEntryImpl that = (QueryResultEntryImpl) o;
        if (!indexKey.equals(that.indexKey)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return indexKey.hashCode();
    }

    public Data getKeyData() {
        return keyData;
    }

    public Data getValueData() {
        return valueData;
    }

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
}
