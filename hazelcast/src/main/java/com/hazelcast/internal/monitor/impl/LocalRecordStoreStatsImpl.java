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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.monitor.LocalRecordStoreStats;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class LocalRecordStoreStatsImpl
        implements LocalRecordStoreStats, IdentifiedDataSerializable {

    private long hits;
    private long lastAccess;
    private long lastUpdate;

    public void copyFrom(LocalRecordStoreStats stats) {
        this.hits = stats.getHits();
        this.lastAccess = stats.getLastAccessTime();
        this.lastUpdate = stats.getLastUpdateTime();
    }

    @Override
    public long getHits() {
        return hits;
    }

    @Override
    public long getLastAccessTime() {
        return lastAccess;
    }

    @Override
    public long getLastUpdateTime() {
        return lastUpdate;
    }

    @Override
    public void increaseHits() {
        this.hits++;
    }

    @Override
    public void increaseHits(long hits) {
        this.hits += hits;
    }

    @Override
    public void decreaseHits(long hits) {
        this.hits -= hits;
    }

    @Override
    public void setLastAccessTime(long time) {
        this.lastAccess = Math.max(this.lastAccess, time);
    }

    @Override
    public void setLastUpdateTime(long time) {
        this.lastUpdate = Math.max(this.lastUpdate, time);
    }

    public void reset() {
        this.hits = 0;
        this.lastAccess = 0;
        this.lastUpdate = 0;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(hits);
        out.writeLong(lastAccess);
        out.writeLong(lastUpdate);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        hits = in.readLong();
        lastAccess = in.readLong();
        lastUpdate = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.LOCAL_RECORD_STORE_STATS;
    }
}
