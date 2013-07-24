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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class RecordReplicationInfo implements DataSerializable {

    private Record record;
    private long idleDelayMillis = -1;
    private long ttlDelayMillis = -1;
    private long mapStoreWriteDelayMillis = -1;
    private long mapStoreDeleteDelayMillis = -1;

    public RecordReplicationInfo(Record record, long idleDelayMillis, long ttlDelayMillis, long mapStoreWriteDelayMillis, long mapStoreDeleteDelayMillis) {
        this.record = record;
        this.idleDelayMillis = idleDelayMillis;
        this.ttlDelayMillis = ttlDelayMillis;
        this.mapStoreWriteDelayMillis = mapStoreWriteDelayMillis;
        this.mapStoreDeleteDelayMillis = mapStoreDeleteDelayMillis;
    }

    public RecordReplicationInfo(Record record) {
        this.record = record;
    }

    public RecordReplicationInfo() {
    }

    public Record getRecord() {
        return record;
    }

    public long getIdleDelayMillis() {
        return idleDelayMillis;
    }

    public long getTtlDelayMillis() {
        return ttlDelayMillis;
    }

    public long getMapStoreWriteDelayMillis() {
        return mapStoreWriteDelayMillis;
    }

    public long getMapStoreDeleteDelayMillis() {
        return mapStoreDeleteDelayMillis;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(record);
        out.writeLong(idleDelayMillis);
        out.writeLong(ttlDelayMillis);
        out.writeLong(mapStoreWriteDelayMillis);
        out.writeLong(mapStoreDeleteDelayMillis);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        record = in.readObject();
        idleDelayMillis = in.readLong();
        ttlDelayMillis = in.readLong();
        mapStoreWriteDelayMillis = in.readLong();
        mapStoreDeleteDelayMillis = in.readLong();
    }
}
