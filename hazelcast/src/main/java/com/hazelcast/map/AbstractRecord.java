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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import sun.text.normalizer.VersionInfo;

import java.io.IOException;



@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord implements Record, DataSerializable {

    // todo remove id
    protected volatile long id;
    protected volatile RecordState state;
    protected volatile RecordStats stats;
    protected volatile Data keyData;

    public AbstractRecord(long id, Data keyData) {
        this.id = id;
        this.keyData = keyData;
        state = new RecordState();
        stats = new RecordStats();
    }

    public AbstractRecord() {
    }

    public long getId() {
        return id;
    }

    public Data getKey() {
        return keyData;
    }

    public RecordState getState() {
        return state;
    }

    public Long getLastAccessTime() {
        return stats == null ? 0 : stats.getLastAccessTime();
    }

    public void access() {
        if(stats != null)
            stats.access();
    }

    public Record clone() {
        // todo implement
        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(id);
        if(state != null) {
            out.writeBoolean(true);
            state.writeData(out);
        }
        else {
            out.writeBoolean(false);
        }

        if(stats != null) {
            out.writeBoolean(true);
            stats.writeData(out);
        }
        else {
            out.writeBoolean(false);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        id = in.readLong();
        boolean stateEnabled = in.readBoolean();
        if(stateEnabled) {
            state = new RecordState();
            state.readData(in);
        }
        boolean statsEnabled = in.readBoolean();
        if(statsEnabled) {
            stats = new RecordStats();
            stats.readData(in);
        }
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractRecord)) return false;
        Record record = (Record) o;
        return record.getId() == getId();
    }

    public String toString() {
        return "Record id=" + getId();
    }

    public int hashCode() {
        return (int) (id ^ (id >>> 32));
    }


    public Integer getHits() {
        return stats == null ? 0 : stats.getHits();
    }
}
