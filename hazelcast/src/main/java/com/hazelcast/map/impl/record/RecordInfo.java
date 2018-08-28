/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.record;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.impl.Versioned;

import java.io.IOException;

import static com.hazelcast.internal.cluster.Versions.V3_11;
import static com.hazelcast.map.impl.record.Record.NOT_AVAILABLE;

/**
 * Record info.
 */
public class RecordInfo
        implements IdentifiedDataSerializable, Versioned {
    protected long version;
    protected long ttl;
    protected long maxIdle;
    protected long creationTime;
    protected long lastAccessTime;
    protected long lastUpdateTime;
    protected long hits;
    // available when stats are enabled.
    protected long lastStoredTime;
    protected long expirationTime;

    public RecordInfo() {
    }

    public RecordInfo(RecordInfo recordInfo) {
        this.version = recordInfo.version;
        this.hits = recordInfo.hits;
        this.ttl = recordInfo.ttl;
        this.maxIdle = recordInfo.maxIdle;
        this.creationTime = recordInfo.creationTime;
        this.lastAccessTime = recordInfo.lastAccessTime;
        this.lastUpdateTime = recordInfo.lastUpdateTime;

        // available when stats are enabled.
        this.lastStoredTime = recordInfo.lastStoredTime;
        this.expirationTime = recordInfo.expirationTime;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getHits() {
        return hits;
    }

    public void setHits(long hits) {
        this.hits = hits;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public long getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(long maxIdle) {
        this.maxIdle = maxIdle;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(version);
        out.writeLong(hits);
        out.writeLong(ttl);
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(lastUpdateTime);

        boolean statsEnabled = !(lastStoredTime == NOT_AVAILABLE && expirationTime == NOT_AVAILABLE);
        out.writeBoolean(statsEnabled);
        if (statsEnabled) {
            out.writeLong(lastStoredTime);
            out.writeLong(expirationTime);
        }

        //RU_COMPAT_3_10
        if (out.getVersion().isGreaterOrEqual(V3_11)) {
            out.writeLong(maxIdle);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        version = in.readLong();
        hits = in.readLong();
        ttl = in.readLong();
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        lastUpdateTime = in.readLong();

        boolean statsEnabled = in.readBoolean();
        lastStoredTime = statsEnabled ? in.readLong() : NOT_AVAILABLE;
        expirationTime = statsEnabled ? in.readLong() : NOT_AVAILABLE;

        //RU_COMPAT_3_10
        if (in.getVersion().isGreaterOrEqual(V3_11)) {
            maxIdle = in.readLong();
        }

    }

    @Override
    public String toString() {
        return "RecordInfo{"
                + "creationTime=" + creationTime
                + ", version=" + version
                + ", ttl=" + ttl
                + ", maxIdle=" + maxIdle
                + ", lastAccessTime=" + lastAccessTime
                + ", lastUpdateTime=" + lastUpdateTime
                + ", hits=" + hits
                + ", lastStoredTime=" + lastStoredTime
                + ", expirationTime=" + expirationTime
                + '}';
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getId() {
        return MapDataSerializerHook.RECORD_INFO;
    }
}
