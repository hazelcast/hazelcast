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

import com.hazelcast.nio.Data;
import com.hazelcast.util.Clock;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import static com.hazelcast.nio.IOUtil.toObject;



@SuppressWarnings("VolatileLongOrDoubleField")
public abstract class AbstractRecord extends AbstractSimpleRecord implements Record {

    protected volatile long maxIdleMillis = Long.MAX_VALUE;
    protected volatile long lastAccessTime = 0;
    protected volatile long creationTime = 0;
    protected volatile long ttl = -1;

    protected volatile boolean dirty = false;
    protected volatile boolean active = true;

    public AbstractRecord(long id, Data key, Data value, long ttl, long maxIdleMillis) {
        super(id, key, value);
        this.ttl = ttl;
        this.creationTime = Clock.currentTimeMillis();
        this.maxIdleMillis = (maxIdleMillis == 0) ? Long.MAX_VALUE : maxIdleMillis;
    }

    public boolean isDirty() {
        return dirty;
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isActive() {
        return active;
    }

    public void setActive(boolean active) {
        this.active = active;
    }

    @Override
    public void writeData(DataOutput out) throws IOException {
        super.writeData(out);
        out.writeLong(creationTime);
        out.writeLong(lastAccessTime);
        out.writeLong(maxIdleMillis);
        out.writeLong(ttl);
        out.writeBoolean(dirty);
        out.writeBoolean(active);
    }

    @Override
    public void readData(DataInput in) throws IOException {
        super.readData(in);
        creationTime = in.readLong();
        lastAccessTime = in.readLong();
        maxIdleMillis = in.readLong();
        ttl = in.readLong();
        dirty = in.readBoolean();
        active = in.readBoolean();
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AbstractRecord)) return false;
        Record record = (Record) o;
        return record.getId() == getId();
    }

    public String toString() {
        return "Record key=" + getKeyData();
    }

}
