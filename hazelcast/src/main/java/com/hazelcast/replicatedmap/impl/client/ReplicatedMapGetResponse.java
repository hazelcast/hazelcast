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

package com.hazelcast.replicatedmap.impl.client;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;

import java.io.IOException;

/**
 * Response class for client replicated map get requests
 */
public class ReplicatedMapGetResponse
        implements Portable {

    private Object value;
    private long ttlMillis;
    private long updateTime;

    ReplicatedMapGetResponse() {
    }

    ReplicatedMapGetResponse(Object value, long ttlMillis, long updateTime) {
        this.value = value;
        this.ttlMillis = ttlMillis;
        this.updateTime = updateTime;
    }

    public Object getValue() {
        return value;
    }

    public long getTtlMillis() {
        return ttlMillis;
    }

    public long getUpdateTime() {
        return updateTime;
    }

    @Override
    public void writePortable(PortableWriter writer)
            throws IOException {
        writer.writeLong("ttlMillis", ttlMillis);
        writer.writeLong("updateTime", updateTime);
        ObjectDataOutput out = writer.getRawDataOutput();
        out.writeObject(value);
    }

    @Override
    public void readPortable(PortableReader reader)
            throws IOException {
        ttlMillis = reader.readLong("ttlMillis");
        updateTime = reader.readLong("updateTime");
        ObjectDataInput in = reader.getRawDataInput();
        value = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return ReplicatedMapPortableHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ReplicatedMapPortableHook.GET_RESPONSE;
    }

}
