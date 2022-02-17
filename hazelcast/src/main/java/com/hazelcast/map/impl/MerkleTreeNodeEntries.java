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

package com.hazelcast.map.impl;

import com.hazelcast.internal.util.collection.InflatableSet;
import com.hazelcast.internal.util.collection.InflatableSet.Builder;
import com.hazelcast.map.impl.wan.WanMapEntryView;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.wan.impl.merkletree.MerkleTree;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * The map entries that belong to a specific merkle tree node. The merkle
 * tree node is identified by the node order.
 *
 * @see MerkleTree
 */
public class MerkleTreeNodeEntries implements IdentifiedDataSerializable {
    private int nodeOrder;
    private Set<WanMapEntryView<Object, Object>> nodeEntries = Collections.emptySet();

    public MerkleTreeNodeEntries() {
    }

    public MerkleTreeNodeEntries(int nodeOrder, Set<WanMapEntryView<Object, Object>> nodeEntries) {
        this.nodeOrder = nodeOrder;
        this.nodeEntries = nodeEntries;
    }

    public Set<WanMapEntryView<Object, Object>> getNodeEntries() {
        return nodeEntries;
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.MERKLE_TREE_NODE_ENTRIES;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(nodeOrder);
        out.writeInt(nodeEntries.size());
        for (WanMapEntryView<Object, Object> entryView : nodeEntries) {
            out.writeObject(entryView);
        }
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        nodeOrder = in.readInt();
        int entryCount = in.readInt();
        Builder<WanMapEntryView<Object, Object>> entries = InflatableSet.newBuilder(entryCount);
        for (int j = 0; j < entryCount; j++) {
            entries.add(in.readObject());
        }
        nodeEntries = entries.build();
    }
}
