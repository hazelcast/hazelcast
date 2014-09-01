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

package com.hazelcast.map.impl.record;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;
import java.io.IOException;

/**
 * TODO empty statistics.
 * Some statistics of a {@link com.hazelcast.map.impl.record.Record}
 */
public class RecordStatistics implements DataSerializable {

    // TODO is volatile needed? if yes then hits should be atomicnumber
    protected int hits;
    protected long lastStoredTime;
    protected long expirationTime;

    public RecordStatistics() {

    }

    public int getHits() {
        return hits;
    }

    public void setHits(int hits) {
        this.hits = hits;
    }

    public long getExpirationTime() {
        return expirationTime;
    }

    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    public void access() {
        hits++;
    }

    public void store() {
        lastStoredTime = Clock.currentTimeMillis();
    }

    public long getLastStoredTime() {
        return lastStoredTime;
    }

    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    public long size() {
        //size of the instance.
        final int numberOfLongVariables = 3;
        return numberOfLongVariables * (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(hits);
        out.writeLong(lastStoredTime);
        out.writeLong(expirationTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        hits = in.readInt();
        lastStoredTime = in.readLong();
        expirationTime = in.readLong();
    }


}
