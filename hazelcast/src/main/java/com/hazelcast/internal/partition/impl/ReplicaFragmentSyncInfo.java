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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.services.ServiceNamespace;

/**
 * The information for a replica synchronization - which partition and replica index needs synchronization and what is
 * the target (the owner of the partition).
 * The target is ignored when comparing if two {@link ReplicaFragmentSyncInfo} instances are the same.
 */
public final class ReplicaFragmentSyncInfo {

    final int partitionId;
    final ServiceNamespace namespace;
    final int replicaIndex;

    // Intentionally not used in equals and hashCode.
    final PartitionReplica target;

    ReplicaFragmentSyncInfo(int partitionId, ServiceNamespace namespace, int replicaIndex,
            PartitionReplica target) {
        this.partitionId = partitionId;
        this.namespace = namespace;
        this.replicaIndex = replicaIndex;
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicaFragmentSyncInfo that = (ReplicaFragmentSyncInfo) o;
        return partitionId == that.partitionId && replicaIndex == that.replicaIndex && namespace.equals(that.namespace);
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + namespace.hashCode();
        result = 31 * result + replicaIndex;
        return result;
    }

    @Override
    public String toString() {
        return "ReplicaFragmentSyncInfo{" + "partitionId=" + partitionId + ", namespace=" + namespace
                + ", replicaIndex=" + replicaIndex + ", target=" + target + '}';
    }
}
