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

/**
 * @author enesakar 11/25/13
 */
public class RecordInfo implements DataSerializable {
    protected RecordStatistics statistics;
    protected long idleDelayMillis = -1;
    protected long ttlDelayMillis = -1;
    protected long mapStoreWriteDelayMillis = -1;
    protected long mapStoreDeleteDelayMillis = -1;

    public RecordInfo() {
    }

    public RecordInfo(RecordStatistics statistics,
                      long idleDelayMillis, long ttlDelayMillis, long mapStoreWriteDelayMillis,
                      long mapStoreDeleteDelayMillis) {
        this.statistics = statistics;
        this.idleDelayMillis = idleDelayMillis;
        this.ttlDelayMillis = ttlDelayMillis;
        this.mapStoreWriteDelayMillis = mapStoreWriteDelayMillis;
        this.mapStoreDeleteDelayMillis = mapStoreDeleteDelayMillis;
    }

    public RecordInfo(RecordStatistics statistics,
                      long idleDelayMillis, long ttlDelayMillis) {
        this(statistics, idleDelayMillis, ttlDelayMillis, -1, -1);
    }


    public RecordStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(RecordStatistics statistics) {
        this.statistics = statistics;
    }

    public long getIdleDelayMillis() {
        return idleDelayMillis;
    }

    public void setIdleDelayMillis(long idleDelayMillis) {
        this.idleDelayMillis = idleDelayMillis;
    }

    public long getTtlDelayMillis() {
        return ttlDelayMillis;
    }

    public void setTtlDelayMillis(long ttlDelayMillis) {
        this.ttlDelayMillis = ttlDelayMillis;
    }

    public long getMapStoreWriteDelayMillis() {
        return mapStoreWriteDelayMillis;
    }

    public void setMapStoreWriteDelayMillis(long mapStoreWriteDelayMillis) {
        this.mapStoreWriteDelayMillis = mapStoreWriteDelayMillis;
    }

    public long getMapStoreDeleteDelayMillis() {
        return mapStoreDeleteDelayMillis;
    }

    public void setMapStoreDeleteDelayMillis(long mapStoreDeleteDelayMillis) {
        this.mapStoreDeleteDelayMillis = mapStoreDeleteDelayMillis;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (statistics != null) {
            out.writeBoolean(true);
            statistics.writeData(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeLong(idleDelayMillis);
        out.writeLong(ttlDelayMillis);
        out.writeLong(mapStoreWriteDelayMillis);
        out.writeLong(mapStoreDeleteDelayMillis);

    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        boolean statsEnabled = in.readBoolean();
        if (statsEnabled) {
            statistics = new RecordStatistics();
            statistics.readData(in);
        }
        idleDelayMillis = in.readLong();
        ttlDelayMillis = in.readLong();
        mapStoreWriteDelayMillis = in.readLong();
        mapStoreDeleteDelayMillis = in.readLong();
    }

    @Override
    public String toString() {
        return "RecordInfo{" +
                "statistics=" + statistics +
                ", idleDelayMillis=" + idleDelayMillis +
                ", ttlDelayMillis=" + ttlDelayMillis +
                ", mapStoreWriteDelayMillis=" + mapStoreWriteDelayMillis +
                ", mapStoreDeleteDelayMillis=" + mapStoreDeleteDelayMillis +
                '}';
    }
}
