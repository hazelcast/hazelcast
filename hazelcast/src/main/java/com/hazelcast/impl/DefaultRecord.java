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

import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toObject;

@SuppressWarnings("SynchronizeOnThis")
public final class DefaultRecord extends AbstractRecord {

    private volatile Data valueData;
    protected volatile Object valueObject;

    public DefaultRecord(long id, Data keyData, Data valueData, long ttl, long maxIdleMillis) {
        super(id, keyData, ttl, maxIdleMillis);
        this.valueData = valueData;
    }

    public DefaultRecord() {
        super();
    }

    public Object getValue() {
        if (valueObject == null)
            valueObject = toObject(valueData);
        return valueObject;
    }

    public Object setValue(Object value) {
        Object oldValue = getValue();
        valueObject = value;
        return oldValue;
    }

    public Data getValueData() {
        return valueData;
    }

    public void setValueData(Data valueData) {
        this.valueData = valueData;
        valueObject = null;
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        keyData.writeData(out);
        valueData.writeData(out);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        keyData = new Data();
        keyData.readData(in);
        valueData = new Data();
        valueData.readData(in);
    }

}
