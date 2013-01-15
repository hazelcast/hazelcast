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
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;



@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord implements DataSerializable {

    protected volatile RecordState state;
    protected volatile RecordStats stats;
    protected volatile Data key;

    public AbstractRecord(Data key) {
        this.key = key;
        state = new RecordState();
        stats = new RecordStats();
    }

    public AbstractRecord() {
    }

    public Data getKey() {
        return key;
    }

    public RecordState getState() {
        return state;
    }

    public void setState(RecordState state) {
        this.state = state;
    }

    public RecordStats getStats() {
        return stats;
    }

    public void setStats(RecordStats stats) {
        this.stats = stats;
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
        key.writeData(out);
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
        key = new Data();
        key.readData(in);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AbstractRecord that = (AbstractRecord) o;

        if (!key.equals(that.key)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return "AbstractRecord{" +
                "key=" + key +
                '}';
    }

    public Integer getHits() {
        return stats == null ? 0 : stats.getHits();
    }
}
