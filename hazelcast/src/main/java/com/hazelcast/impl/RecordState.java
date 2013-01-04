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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

public class RecordState implements DataSerializable {

    protected volatile long maxIdleMillis = Long.MAX_VALUE;
    protected volatile long lastAccessTime = 0;
    protected volatile long creationTime = 0;
    protected volatile long ttl = -1;

    protected volatile boolean dirty = false;
    protected volatile boolean active = true;

    public long getMaxIdleMillis() {
        return maxIdleMillis;
    }

    public void setMaxIdleMillis(long maxIdleMillis) {
        this.maxIdleMillis = maxIdleMillis;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime) {
        this.lastAccessTime = lastAccessTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isActive() {
        // check if record has expired, if so make active false
        if(active && ttl > 0 && Clock.currentTimeMillis() > creationTime + ttl)
            active = false;
        if(active && maxIdleMillis > 0 && Clock.currentTimeMillis() > lastAccessTime + maxIdleMillis)
            active = false;
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(maxIdleMillis);
        out.writeLong(lastAccessTime);
        out.writeLong(creationTime);
        out.writeLong(ttl);
        out.writeBoolean(dirty);
        out.writeBoolean(active);
    }

    public void readData(ObjectDataInput in) throws IOException {
        maxIdleMillis = in.readLong();
        lastAccessTime = in.readLong();
        creationTime = in.readLong();
        ttl = in.readLong();
        dirty = in.readBoolean();
        active = in.readBoolean();
    }
}
