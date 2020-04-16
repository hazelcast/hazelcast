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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.partition.PartitionReplicaInterceptor;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public final class PerMemberPartitionReplicaInterceptor implements PartitionReplicaInterceptor {

    private final AtomicInteger stateVersion;
    private final Map<UUID, List<Integer>> map = new HashMap<>();

    public PerMemberPartitionReplicaInterceptor(AtomicInteger stateVersion) {
        this.stateVersion = stateVersion;
    }

    @Override
    public void replicaChanged(int partitionId, int replicaIndex,
                               PartitionReplica oldReplica, PartitionReplica newReplica) {
        assert newReplica != null : "newReplica is null";

        // TODO can both uuid or one of them be null?
        UUID oldUuid = oldReplica != null ? oldReplica.uuid() : null;
        UUID newUuid = newReplica.uuid();

        if (oldUuid != null) {
            map.computeIfPresent(oldUuid, (uuid, partitions) -> {
                // TODO  put a more appropriate collection, since remove is not O(1)
                partitions.remove(partitionId);
                return partitions.isEmpty() ? null : partitions;
            });
        }

        if (newUuid != null) {
            map.compute(newUuid, (uuid, partitions) -> {
                if (partitions == null) {
                    partitions = new LinkedList<>();
                }
                partitions.add(partitionId);
                return partitions;
            });

        }
    }

    public PartitionsInfo getPartitionsInfo() {
        return new PartitionsInfo(stateVersion.get(), map);
    }

    public static class PartitionsInfo {
        private final int partitionStateVersion;
        private final Map<UUID, List<Integer>> partitionIdByUuid;

        public PartitionsInfo(int partitionStateVersion, Map<UUID, List<Integer>> partitionIdByUuid) {
            this.partitionStateVersion = partitionStateVersion;
            this.partitionIdByUuid = partitionIdByUuid;
        }

        public int getPartitionStateVersion() {
            return partitionStateVersion;
        }

        public Map<UUID, List<Integer>> getPartitionIdByUuid() {
            return partitionIdByUuid;
        }
    }
}
