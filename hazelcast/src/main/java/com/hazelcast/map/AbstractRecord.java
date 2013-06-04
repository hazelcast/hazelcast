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
public abstract class AbstractRecord<V> implements Record<V>, DataSerializable {

    // todo ea volatile is needed? if yes then make version atomic
    protected volatile RecordStatistics statistics;
    protected volatile Data key;
    protected volatile long version;

    public AbstractRecord(Data key, boolean statisticsEnabled) {
        this.key = key;
        if (statisticsEnabled) {
            statistics = new RecordStatistics();
        }
        version = 0;
    }

    public AbstractRecord() {
    }

    public Data getKey() {
        return key;
    }

    public RecordStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(RecordStatistics stats) {
        this.statistics = stats;
    }

    public Integer getHits() {
        return statistics == null ? -1 : statistics.getHits();
    }

    public Long getLastAccessTime() {
        return statistics == null ? -1 : statistics.getLastAccessTime();
    }

    public abstract long getCost();

    public long getVersion() {
        return version;
    }

    public void onAccess() {
        if (statistics != null)
            statistics.access();
    }

    public void onStore() {
        if (statistics != null)
            statistics.store();
    }

    public void onUpdate() {
        if (statistics != null) {
            statistics.update();
        }
        version++;
    }

    public Record clone() {
        // todo implement
        return null;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        key.writeData(out);
        if (statistics != null) {
            out.writeBoolean(true);
            statistics.writeData(out);
        } else {
            out.writeBoolean(false);
        }
    }

    public void readData(ObjectDataInput in) throws IOException {
        key = new Data();
        key.readData(in);
        boolean statsEnabled = in.readBoolean();
        if (statsEnabled) {
            statistics = new RecordStatistics();
            statistics.readData(in);
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

}
