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

package com.hazelcast.internal.crdt.pncounter.operations;

import com.hazelcast.cluster.impl.VectorClock;
import com.hazelcast.internal.crdt.CRDTDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Response for CRDT PN counter operations which contains the PN counter
 * value as well as the current replica vector clock. The vector clock is
 * returned for maintaining session consistency guarantees.
 */
public class CRDTTimestampedLong implements IdentifiedDataSerializable {
    private long value;
    private VectorClock vectorClock;

    public CRDTTimestampedLong() {
    }

    public CRDTTimestampedLong(long value, VectorClock vectorClock) {
        this.value = value;
        this.vectorClock = vectorClock;
    }

    public long getValue() {
        return value;
    }

    public VectorClock getVectorClock() {
        return vectorClock;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public void setVectorClock(VectorClock vectorClock) {
        this.vectorClock = vectorClock;
    }

    @Override
    public int getFactoryId() {
        return CRDTDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CRDTDataSerializerHook.CRDT_TIMESTAMPED_LONG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(value);
        out.writeObject(vectorClock);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        value = in.readLong();
        vectorClock = in.readObject();
    }
}
