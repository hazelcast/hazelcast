/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.compatibility.map;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.internal.util.collection.InflatableSet.Builder;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * A compatibility (3.x) version of {@link com.hazelcast.map.impl.MerkleTreeNodeEntries}.
 */
public class CompatibilityMerkleTreeNodeEntries implements IdentifiedDataSerializable {
    private Set<CompatibilityWanMapEntryView<Data, Data>> nodeEntries = Collections.emptySet();

    public CompatibilityMerkleTreeNodeEntries() {
    }

    public Set<CompatibilityWanMapEntryView<Data, Data>> getNodeEntries() {
        return nodeEntries;
    }

    @Override
    public int getFactoryId() {
        return CompatibilityMapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return CompatibilityMapDataSerializerHook.MERKLE_TREE_NODE_ENTRIES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        throw new UnsupportedOperationException(getClass().getName() + " should not be serialized!");
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        // nodeOrder is not needed
        in.readInt();
        int entryCount = in.readInt();
        Builder<CompatibilityWanMapEntryView<Data, Data>> entries = InflatableSet.newBuilder(entryCount);
        for (int j = 0; j < entryCount; j++) {
            entries.add(in.readObject());
        }
        nodeEntries = entries.build();
    }
}
