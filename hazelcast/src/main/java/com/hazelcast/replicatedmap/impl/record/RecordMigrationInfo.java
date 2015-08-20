/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import java.io.IOException;

public class RecordMigrationInfo implements DataSerializable {

    private Data key;
    private Data value;
    private long ttl;

    public RecordMigrationInfo(Data key, Data value, long ttl) {
        this.key = key;
        this.value = value;
        this.ttl = ttl;
    }

    public RecordMigrationInfo() {
    }

    public Data getKey() {
        return key;
    }

    public Data getValue() {
        return value;
    }

    public long getTtl() {
        return ttl;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeData(key);
        out.writeData(value);
        out.writeLong(ttl);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        key = in.readData();
        value = in.readData();
        ttl = in.readLong();
    }

    @Override
    public String toString() {
        return "RecordMigrationInfo{"
                + "key=" + key
                + ", value=" + value
                + "} ";
    }
}
