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

package com.hazelcast.cp.internal.datastructures.atomicref;

import com.hazelcast.cp.internal.datastructures.spi.atomic.RaftAtomicValueSnapshot;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot of a {@link AtomicRefService} state for a Raft group
 */
public class AtomicRefSnapshot extends RaftAtomicValueSnapshot<Data> implements IdentifiedDataSerializable {

    public AtomicRefSnapshot() {
    }

    public AtomicRefSnapshot(Map<String, Data> refs, Set<String> destroyed) {
        super(refs, destroyed);
    }

    @Override
    public int getFactoryId() {
        return AtomicRefDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return AtomicRefDataSerializerHook.SNAPSHOT;
    }

    @Override
    protected void writeValue(ObjectDataOutput out, Data value) throws IOException {
        IOUtil.writeData(out, value);
    }

    @Override
    protected Data readValue(ObjectDataInput in) throws IOException {
        return IOUtil.readData(in);
    }

    @Override
    public String toString() {
        return "AtomicRefSnapshot{" + "refs=" + values + ", destroyed=" + destroyed + '}';
    }
}
