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

package com.hazelcast.map;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.util.Clock;

import java.io.IOException;

public class RecordState implements DataSerializable {

    protected volatile long ttlExpireTime = 0;
    protected volatile long idleExpireTime = 0;
    protected volatile long storeTime = 0;


    public boolean isExpired() {
        return (ttlExpireTime > 0 && ttlExpireTime < Clock.currentTimeMillis())
                || (idleExpireTime > 0 && idleExpireTime < Clock.currentTimeMillis());
    }

    public boolean isDirty() {
        return storeTime > 0 && storeTime < Clock.currentTimeMillis();
    }

    public long getTtlExpireTime() {
        return ttlExpireTime;
    }

    public long getIdleExpireTime() {
        return idleExpireTime;
    }

    public void resetStoreTime() {
        storeTime = 0;
    }

    // todo is this required
    public void resetExpiration() {
        ttlExpireTime = 0;
        idleExpireTime = 0;
    }

    public long getStoreTime() {
        return storeTime;
    }

    public void updateTtlExpireTime(long ttl) {
        this.ttlExpireTime = Clock.currentTimeMillis() + ttl;
    }

    public void updateStoreTime(long writeDelay) {
        this.storeTime = Clock.currentTimeMillis() + writeDelay;
    }

    public void updateIdleExpireTime(long maxIdle) {
        this.idleExpireTime = Clock.currentTimeMillis() + maxIdle;
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(ttlExpireTime);
        out.writeLong(idleExpireTime);
        out.writeLong(storeTime);
    }

    public void readData(ObjectDataInput in) throws IOException {
        ttlExpireTime = in.readLong();
        idleExpireTime = in.readLong();
        storeTime = in.readLong();
    }
}
