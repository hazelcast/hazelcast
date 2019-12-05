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

package com.hazelcast.map.impl.query;

import com.hazelcast.map.impl.MapDataSerializerHook;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

import static com.hazelcast.map.impl.query.Target.TargetMode.PARTITION_OWNER;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Target for a query.
 * <p>
 * Possible options:
 * - all nodes
 * - local node only
 * - given partition
 */
public class Target implements IdentifiedDataSerializable {

    public static final Target ALL_NODES = new Target(TargetMode.ALL_NODES, null);
    public static final Target LOCAL_NODE = new Target(TargetMode.LOCAL_NODE, null);

    private TargetMode mode;
    private Integer partitionId;

    public Target() {
    }

    private Target(TargetMode mode, Integer partitionId) {
        this.mode = checkNotNull(mode);
        this.partitionId = partitionId;
        if (mode.equals(PARTITION_OWNER) && partitionId == null) {
            throw new IllegalArgumentException("It's forbidden to use null partitionId with PARTITION_OWNER mode");
        }
    }

    public TargetMode mode() {
        return mode;
    }

    public Integer partitionId() {
        return partitionId;
    }

    enum TargetMode {
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
        out.writeInt(partitionId);
        out.writeUTF(mode.name());
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        this.partitionId = in.readInt();
        this.mode = TargetMode.valueOf(in.readUTF());
    }

    public static Target createPartitionTarget(int partitionId) {
        return new Target(TargetMode.PARTITION_OWNER, partitionId);
    }
}
