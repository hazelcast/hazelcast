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

package com.hazelcast.map.record;

import com.hazelcast.map.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public /*final*/ class DataRecord extends AbstractRecord<Data> implements Record<Data>, IdentifiedDataSerializable {

    protected volatile Data value;

    public DataRecord(Data keyData, Data value, boolean statisticsEnabled) {
        super(keyData, statisticsEnabled);
        this.value = value;
    }

    public DataRecord() {
    }

    /*
    * get record size in bytes.
    *
    * */
    @Override
    public long getCost() {

        long size = 0;

        // add statistics size if enabled.
        size += ( statistics == null ? 0 : statistics.size() );

        // add size of version.
        size += ( Long.SIZE/Byte.SIZE );

        // add key size.
        size += key.totalSize();

        // add value size.
        size += ( value == null ? 0 : value.totalSize() );


        return size;
    }

    public Data getValue() {
        return value;
    }

    public Data setValue(Data o) {
        Data old = value;
        this.value = o;
        return old;
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

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.DATA_RECORD;
    }
}
