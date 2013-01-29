/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class QueryResultEntryImpl implements DataSerializable, QueryResultEntry {
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
        getIndexKey().writeData(out);
        getKeyData().writeData(out);
        getValueData().writeData(out);
    }

    public void readData(ObjectDataInput in) throws IOException {
        indexKey = new Data();
        indexKey.readData(in);
        keyData = new Data();
        keyData.readData(in);
        valueData = new Data();
        valueData.readData(in);
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
}
