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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.query.Target.TargetMode.PARTITION_OWNER;

/**
 * Target for a query.
 * <p>
 * Possible options:<ul>
 *     <li>all nodes
 *     <li>local node only
 *     <li>given partition set
 * </ul>
 */
public class Target implements IdentifiedDataSerializable {

    public static final Target ALL_NODES = new Target(TargetMode.ALL_NODES, null);
    public static final Target LOCAL_NODE = new Target(TargetMode.LOCAL_NODE, null);

    private TargetMode mode;
    private PartitionIdSet partitions;

    public Target() {
    }

    private Target(TargetMode mode, PartitionIdSet partitions) {
        this.mode = checkNotNull(mode);
        this.partitions = partitions;
        if (mode.equals(PARTITION_OWNER) ^ partitions != null) {
            throw new IllegalArgumentException("partitions must be used only with PARTITION_OWNER mode and not otherwise");
        }
    }

    public TargetMode mode() {
        return mode;
    }

    public PartitionIdSet partitions() {
        return partitions;
    }

    public enum TargetMode {
        LOCAL_NODE,
        ALL_NODES,
        PARTITION_OWNER
    }

    @Override
    public int getFactoryId() {
        return MapDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return MapDataSerializerHook.TARGET;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        if (true) {
            throw new UnsupportedOperationException();
        }
        // TODO use IDS
        out.writeObject(partitions);
        out.writeUTF(mode.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.partitions = in.readObject();
        this.mode = TargetMode.valueOf(in.readUTF());
    }

    public static Target createPartitionTarget(PartitionIdSet partitions) {
        return new Target(TargetMode.PARTITION_OWNER, partitions);
    }
}
