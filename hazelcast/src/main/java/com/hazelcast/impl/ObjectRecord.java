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

package com.hazelcast.impl;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class ObjectRecord extends AbstractRecord {

    private volatile Object value;

    public ObjectRecord(long id, Data keyData, Data valueData, long ttl, long maxIdleMillis) {
        super(id, keyData, ttl, maxIdleMillis);
//        this.value = IOUtil.toObject(valueData);
    }

    public Data getValueData() {
//        return IOUtil.toData(value);
        return null;
    }

    public void setValueData(Data dataValue) {
//        value = IOUtil.toObject(dataValue);
    }

    public Object getValue() {
        return value;
    }

    public Object setValue(Object o) {
        Record old = ((Record) o).clone();
        value = o;
        return old;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        keyData.writeData(out);
        getValueData().writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        keyData = new Data();
        keyData.readData(in);
        Data valueData = new Data();
        valueData.readData(in);
//        value = IOUtil.toObject(valueData);
    }
}
