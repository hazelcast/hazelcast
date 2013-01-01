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

package com.hazelcast.impl;

import com.hazelcast.nio.DataSerializable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class RecordStats implements DataSerializable {

    protected volatile int hits = 0;
    protected volatile int version = 0;
    protected volatile long writeTime = -1;
    protected volatile long removeTime = 0;
    protected volatile long lastStoredTime = 0;
    protected volatile long expirationTime = Long.MAX_VALUE;
    protected volatile long lastUpdateTime = 0;

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getWriteTime() {
        return writeTime;
    }

    public void setWriteTime(long writeTime) {
        this.writeTime = writeTime;
    }

    public long getRemoveTime() {
        return removeTime;
    }

    public void setRemoveTime(long removeTime) {
        this.removeTime = removeTime;
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public void writeData(DataOutput out) throws IOException {
        out.writeInt(hits);
        out.writeInt(version);
        out.writeLong(writeTime);
        out.writeLong(removeTime);
        out.writeLong(lastStoredTime);
        out.writeLong(expirationTime);
        out.writeLong(lastUpdateTime);
    }

    public void readData(DataInput in) throws IOException {
        hits = in.readInt();
        version = in.readInt();
        writeTime = in.readLong();
        removeTime = in.readLong();
        lastStoredTime = in.readLong();
        expirationTime = in.readLong();
        lastUpdateTime = in.readLong();
    }

}
