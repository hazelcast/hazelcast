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

import com.hazelcast.impl.base.DistributedLock;
import com.hazelcast.impl.concurrentmap.ValueHolder;
import com.hazelcast.map.RecordStats;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toObject;

@SuppressWarnings("SynchronizeOnThis")
public final class DefaultRecord extends AbstractRecord implements DataSerializable{

    protected volatile Object valueObject;
    protected volatile Object keyObject;
    protected volatile RecordStats stats;

    public DefaultRecord(long id, Data keyData, Data valueData, long ttl, long maxIdleMillis) {
        super(id, keyData, valueData, ttl, maxIdleMillis);
    }

    public Object getValue() {
        if(valueObject == null)
            valueObject = toObject(valueData);
        return valueObject;
    }

    public Object getKey() {
        if(keyObject == null)
            keyObject = toObject(keyData);
        return keyObject;
    }

    public Object setValue(Object value) {
        Object oldValue = getValue();
        valueObject = value;
        return oldValue;
    }

    public void setValueData(Data valueData) {
        super.setValueData(valueData);
        valueObject = null;
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        stats.writeData(out);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        stats = new RecordStats();
    }

}
