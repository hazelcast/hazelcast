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

package com.hazelcast.multimap.impl;

import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class MultiMapRecord implements IdentifiedDataSerializable {

    private long recordId = -1;
    private Object object;

    public MultiMapRecord() {
    }

    public MultiMapRecord(Object object) {
        this.object = object;
    }

    public MultiMapRecord(long recordId, Object object) {
        this.recordId = recordId;
        this.object = object;
    }

    public long getRecordId() {
        return recordId;
    }

    public void setRecordId(long recordId) {
        this.recordId = recordId;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MultiMapRecord)) {
            return false;
        }

        MultiMapRecord record = (MultiMapRecord) o;
        return object.equals(record.object);
    }

    @Override
    public int hashCode() {
        return object.hashCode();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(recordId);
        IOUtil.writeObject(out, object);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        recordId = in.readLong();
        object = IOUtil.readObject(in);
    }

    @Override
    public int getFactoryId() {
        return MultiMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MultiMapDataSerializerHook.MULTIMAP_RECORD;
    }
}
