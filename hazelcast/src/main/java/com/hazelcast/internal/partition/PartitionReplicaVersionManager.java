/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition;

import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ReplicaFragmentNamespace;

import java.util.Collection;

/**
 * TODO: Javadoc Pending...
 *
 * @since 3.9
 */
public interface PartitionReplicaVersionManager {

    Collection<ReplicaFragmentNamespace> getNamespaces(int partitionId);

    boolean isPartitionReplicaVersionStale(int partitionId, ReplicaFragmentNamespace namespace,
                                           long[] replicaVersions, int replicaIndex);

    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    long[] getPartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace);

    /**
     * Updates the partition replica version and triggers replica sync if the replica is dirty (e.g. the
     * received version is not expected and this node might have missed an update)
     * @param partitionId the id of the partition for which we received a new version
     * @param replicaVersions the received replica versions
     * @param replicaIndex the index of this replica
     */
    void updatePartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace,
                                        long[] replicaVersions, int replicaIndex);

    // Caution: Returning version array without copying for performance reasons. Callers must not modify this array!
    long[] incrementPartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace, int totalBackupCount);

    ReplicaFragmentNamespace getReplicaFragmentNamespace(Operation operation);
}
