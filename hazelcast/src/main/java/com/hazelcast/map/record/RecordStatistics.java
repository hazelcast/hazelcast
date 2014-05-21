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
import com.hazelcast.util.Clock;

import java.io.IOException;

public class RecordStatistics implements DataSerializable {

    // todo is volatile needed? if yes then hits should be atomicnumber
    protected int hits = 0;
    protected long lastStoredTime = 0;
    protected long lastUpdateTime = 0;
    protected long lastAccessTime = 0;
    protected long creationTime = 0;
    protected long expirationTime = 0;

    public RecordStatistics() {
        long now = Clock.currentTimeMillis();
        lastAccessTime = now;
        lastUpdateTime = now;
        creationTime = now;
    }

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void access() {
        lastAccessTime = Clock.currentTimeMillis();
        hits++;
    }

    public void update() {
        lastUpdateTime = Clock.currentTimeMillis();
    }

    public void store() {
        lastStoredTime = Clock.currentTimeMillis();
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public long size() {
        //size of the instance.
        return 5 * (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(hits);
        out.writeLong(lastStoredTime);
        out.writeLong(lastUpdateTime);
        out.writeLong(lastAccessTime);
        out.writeLong(creationTime);
        out.writeLong(expirationTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        hits = in.readInt();
        lastStoredTime = in.readLong();
        lastUpdateTime = in.readLong();
        lastAccessTime = in.readLong();
        creationTime = in.readLong();
        expirationTime = in.readLong();
    }


}
