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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link RaftAtomicRefService} state for a Raft group
 */
public class RaftAtomicRefSnapshot extends RaftAtomicValueSnapshot<Data> implements IdentifiedDataSerializable {

    public RaftAtomicRefSnapshot() {
    }

    public RaftAtomicRefSnapshot(Map<String, Data> refs, Set<String> destroyed) {
        super(refs, destroyed);
    }

    @Override
    public int getFactoryId() {
        return RaftAtomicReferenceDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return RaftAtomicReferenceDataSerializerHook.SNAPSHOT;
    }

    @Override
    protected void writeValue(ObjectDataOutput out, Data value) throws IOException {
        out.writeData(value);
    }

    @Override
    protected Data readValue(ObjectDataInput in) throws IOException {
        return in.readData();
    }

    @Override
    public String toString() {
        return "RaftAtomicRefSnapshot{" + "refs=" + values + ", destroyed=" + destroyed + '}';
    }
}
