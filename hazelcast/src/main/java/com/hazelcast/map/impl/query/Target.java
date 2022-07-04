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

package com.hazelcast.map.impl.query;

import com.hazelcast.internal.util.collection.PartitionIdSet;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.map.impl.query.Target.TargetMode.PARTITION_OWNER;

/**
 * Target for a query.
 * <p>
 * Possible options:<ul>
 * <li>all nodes
 * <li>local node only
 * <li>given partition set
 * </ul>
 */
public final class Target {

    public static final Target ALL_NODES = new Target(TargetMode.ALL_NODES, null);
    public static final Target LOCAL_NODE = new Target(TargetMode.LOCAL_NODE, null);

    private final TargetMode mode;
    private final PartitionIdSet partitions;

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

    public static Target createPartitionTarget(PartitionIdSet partitions) {
        return new Target(TargetMode.PARTITION_OWNER, partitions);
    }

    @Override
    public String toString() {
        return "Target{"
                + "mode=" + mode
                + ", partitionsSize="
                + (partitions == null ? 0 : partitions.size())
                + '}';
    }
}
