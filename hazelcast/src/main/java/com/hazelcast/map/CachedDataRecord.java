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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;

import java.io.IOException;

public class CachedDataRecord extends AbstractRecord<Data> {

    private volatile Data value;
    private volatile Object cachedValue;

    public CachedDataRecord(Data keyData, Data value, boolean statisticsEnabled) {
        super(keyData, statisticsEnabled);
        this.value = value;
    }

    public Data getValue() {
        return value;
    }

    public Data setValue(Data o) {
        cachedValue = null;
        Data old = null;
        this.value = o;
        return old;
    }

    public Object getCachedValue() {
        return cachedValue;
    }

    public void setCachedValue(Object cachedValue) {
        this.cachedValue = cachedValue;
    }

    @Override
    public long getCost() {
        return key.totalSize() + value.totalSize();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        value.writeData(out);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = new Data();
        value.readData(in);
    }
}
