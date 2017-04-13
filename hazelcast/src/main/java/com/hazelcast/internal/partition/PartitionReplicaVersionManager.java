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
import com.hazelcast.spi.ReplicaFragmentAware;
import com.hazelcast.spi.ReplicaFragmentNamespace;

import java.util.Collection;

/**
 * {@code PartitionReplicaVersionManager} maintains partition replica version handling.
 * It keeps versions for each partition and {@link ReplicaFragmentNamespace} pair.
 *
 * @since 3.9
 */
public interface PartitionReplicaVersionManager {

    /**
     * Returns all registered namespaces for given partition id
     * @param partitionId partition id
     * @return known namespaces for partition id
     */
    Collection<ReplicaFragmentNamespace> getNamespaces(int partitionId);

    /**
     * Returns whether given replica version is behind the current version or not.
     * @param partitionId partition id
     * @param namespace replica namespace
     * @param replicaVersions replica versions
     * @param replicaIndex specific replica index
     * @return true if given version is stale, false otherwise
     */
    boolean isPartitionReplicaVersionStale(int partitionId, ReplicaFragmentNamespace namespace,
                                           long[] replicaVersions, int replicaIndex);

    /**
     * Returns replica versions for given partition and namespace.
     * @param partitionId partition id
     * @param namespace replica namespace
     * @return replica versions
     */
    long[] getPartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace);

    /**
     * Updates the partition replica version and triggers replica sync if the replica is dirty (e.g. the
     * received version is not expected and this node might have missed an update)
     *
     * @param partitionId the id of the partition for which we received a new version
     * @param namespace replica namespace
     * @param replicaVersions the received replica versions
     * @param replicaIndex the index of this replica
     */
    void updatePartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace,
                                        long[] replicaVersions, int replicaIndex);

    /**
     * Increments replica versions for given partition and namespace by the number of backup count.
     *
     * @param partitionId partition id
     * @param namespace replica namespace
     * @param backupCount number of desired backups
     * @return incremented replica versions
     */
    long[] incrementPartitionReplicaVersions(int partitionId, ReplicaFragmentNamespace namespace, int backupCount);

    /**
     * Returns {@link ReplicaFragmentNamespace} for given operation. If operation is instance of
     * {@link com.hazelcast.spi.ReplicaFragmentAware} then {@link ReplicaFragmentAware#getReplicaFragmentNamespace()}
     * will be used. Otherwise {@link InternalReplicaFragmentNamespace} will be returned.
     *
     * @param operation operation
     * @return replica fragment namespace for operation
     */
    ReplicaFragmentNamespace getReplicaFragmentNamespace(Operation operation);
}
