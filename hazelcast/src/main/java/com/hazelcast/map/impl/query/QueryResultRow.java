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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;

/**
 * This interfaces provides methods related to entry of the query result.
 */
public class QueryResultRow implements IdentifiedDataSerializable, Map.Entry<Data, Data> {

    private Data key;
    private Data value;

    // needed for serialization
    public QueryResultRow() {
    }

    public QueryResultRow(Data key, Data valueData) {
        this.key = key;
        this.value = valueData;
    }

    @Override
    public Data getKey() {
        return key;
    }

    @Override
    public Data getValue() {
        return value;
    }

    @Override
    public Data setValue(Data value) {
        return value;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.QUERY_RESULT_ROW;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeData(out, key);
        IOUtil.writeData(out, value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = IOUtil.readData(in);
        value = IOUtil.readData(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        QueryResultRow that = (QueryResultRow) o;

        if (key != null ? !key.equals(that.key) : that.key != null) {
            return false;
        }

        if (value != null ? !value.equals(that.value) : that.value != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        return hashCode(key) * 31 + hashCode(value);
    }

    private int hashCode(Data data) {
        return data == null ? 0 : data.hashCode();
    }
}
