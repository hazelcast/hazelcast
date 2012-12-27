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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Data;
import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class AbstractSimpleRecord implements Record, DataSerializable {
    protected volatile long id;
    protected volatile Data keyData;
    protected volatile Data valueData;

    public AbstractSimpleRecord(long id, Data keyData, Data valueData) {
        this.id = id;
        this.keyData = keyData;
        this.valueData = valueData;
    }

    public long getId() {
        return id;
    }

    public Data getKeyData() {
        return keyData;
    }

    public Data getValueData() {
        return valueData;
    }

    public void setValueData(Data valueData) {
        this.valueData = valueData;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeLong(id);
        keyData.writeData(out);
        valueData.writeData(out);
    }

    public void readData(DataInput in) throws IOException {
        id = in.readLong();
        keyData = new Data();
        keyData.readData(in);
        valueData = new Data();
        valueData.readData(in);
    }

    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }
}
