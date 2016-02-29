/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.util.Clock;

import java.io.IOException;

/**
 * Statistics of a {@link com.hazelcast.map.impl.record.Record Record}
 */
public class RecordStatisticsImpl implements RecordStatistics {

    protected long lastStoredTime;
    protected long expirationTime;

    public RecordStatisticsImpl() {
    }

    @Override
    public long getExpirationTime() {
        return expirationTime;
    }

    @Override
    public void setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
    }

    @Override
    public void store() {
        lastStoredTime = Clock.currentTimeMillis();
    }

    @Override
    public long getLastStoredTime() {
        return lastStoredTime;
    }

    @Override
    public void setLastStoredTime(long lastStoredTime) {
        this.lastStoredTime = lastStoredTime;
    }

    @Override
    public long getMemoryCost() {
        //size of the instance.
        final int numberOfLongVariables = 3;
        return numberOfLongVariables * (Long.SIZE / Byte.SIZE) + (Integer.SIZE / Byte.SIZE);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(lastStoredTime);
        out.writeLong(expirationTime);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        lastStoredTime = in.readLong();
        expirationTime = in.readLong();
    }


}
