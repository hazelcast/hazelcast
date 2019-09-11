/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cp.internal.datastructures.atomiclong;

import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link RaftAtomicLongService} state for a Raft group
 */
public class RaftAtomicLongSnapshot extends RaftAtomicValueSnapshot<Long> implements IdentifiedDataSerializable {

    public RaftAtomicLongSnapshot() {
    }

    public RaftAtomicLongSnapshot(Map<String, Long> longs, Set<String> destroyed) {
        super(longs, destroyed);
    }

    @Override
    protected void writeValue(ObjectDataOutput out, Long value) throws IOException {
        out.writeLong(value);
    }

    @Override
    protected Long readValue(ObjectDataInput in) throws IOException {
        return in.readLong();
    }

    @Override
    public int getFactoryId() {
        return RaftAtomicLongDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftAtomicLongDataSerializerHook.SNAPSHOT;
    }

    @Override
    public String toString() {
        return "RaftAtomicLongSnapshot{" + "longs=" + values + ", destroyed=" + destroyed + '}';
    }
}
