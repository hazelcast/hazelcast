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

public final class ObjectRecord extends AbstractRecord<Object> implements Record<Object>, IdentifiedDataSerializable {

    private volatile Object value;

    public ObjectRecord(Data keyData, Object value, boolean statisticsEnabled) {
        super(keyData, statisticsEnabled);
        this.value = value;
    }

    public ObjectRecord() {
    }

    // as there is no easy way to calculate the size of Object cost is not implemented for ObjectRecord
    @Override
    public long getCost() {
        long size = 0;

        // add statistics size if enabled.
        //size += ( statistics == null ? 0 : statistics.size() );

        // add size of version.
        //size += ( Long.SIZE/Byte.SIZE );

        // add key size.
        //size += key.totalSize();

        // todo add object size
        //size += ( value == null ? 0 : value.totalSize() );

        return size;
    }

    public Object getValue() {
        return value;
    }

    public Object setValue(Object o) {
        Object old = value;
        value = o;
        return old;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        super.writeData(out);
        out.writeObject(value);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        super.readData(in);
        value = in.readObject();
    }

    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    public int getId() {
        return MapDataSerializerHook.OBJECT_RECORD;
    }
}
