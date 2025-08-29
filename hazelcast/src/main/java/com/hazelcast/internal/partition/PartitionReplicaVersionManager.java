/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.internal.services.ServiceNamespace;
import com.hazelcast.internal.services.ServiceNamespaceAware;

import java.util.Collection;

/**
 * {@code PartitionReplicaVersionManager} maintains partition replica version handling.
 * It keeps versions for each partition and {@link ServiceNamespace} pair.
 *
 * @since 3.9
 */
public interface PartitionReplicaVersionManager {

    /**
     * Returns all registered namespaces for given partition ID
     * @param partitionId partition ID
     * @return known namespaces for partition ID
     */
    Collection<ServiceNamespace> getNamespaces(int partitionId);

    /**
     * Returns whether given replica version is behind the current version or not.
     * @param partitionId partition ID
     * @param namespace replica namespace
     * @param replicaVersions replica versions
     * @param replicaIndex specific replica index
     * @return true if given version is stale, false otherwise
     */
    boolean isPartitionReplicaVersionStale(int partitionId, ServiceNamespace namespace,
                                           long[] replicaVersions, int replicaIndex);

    /**
     * Returns replica versions for given partition and namespace.
     * @param partitionId partition ID
     * @param namespace replica namespace
     * @return replica versions
     */
    long[] getPartitionReplicaVersions(int partitionId, ServiceNamespace namespace);

    /**
     * Returns replica versions for syncing to backup replicas, ensuring any replica versions
     * that are marked explicitly for sync ({@code REQUIRES_SYNC}) are reset. This is necessary
     * when syncing primary to backup replicas (anti-entropy, migration etc), otherwise there is
     * risk of perpetual attempts to sync partition data which may be already in sync.
     *
     * @see     com.hazelcast.internal.partition.impl.PartitionReplicaManager#REQUIRES_SYNC
     */
    long[] getPartitionReplicaVersionsForSync(int partitionId, ServiceNamespace namespace);

    /**
     * Updates the partition replica version and triggers replica sync if the replica is dirty (e.g. the
     * received version is not expected and this node might have missed an update)
     *
     * @param partitionId the ID of the partition for which we received a new version
     * @param namespace replica namespace
     * @param replicaVersions the received replica versions
     * @param replicaIndex the index of this replica
     */
    void updatePartitionReplicaVersions(int partitionId, ServiceNamespace namespace,
                                        long[] replicaVersions, int replicaIndex);

    /**
     * Increments replica versions for given partition and namespace by the number of backup count.
     *
     * @param partitionId partition ID
     * @param namespace replica namespace
     * @param backupCount number of desired backups
     * @return incremented replica versions
     */
    long[] incrementPartitionReplicaVersions(int partitionId, ServiceNamespace namespace, int backupCount);

    /**
     * Returns {@link ServiceNamespace} for given operation. If operation is instance of
     * {@link ServiceNamespaceAware} then {@link ServiceNamespaceAware#getServiceNamespace()}
     * will be used. Otherwise {@link NonFragmentedServiceNamespace} will be returned.
     *
     * @return service namespace for operation
     */
    ServiceNamespace getServiceNamespace(Operation operation);

    void markPartitionReplicaAsSyncRequired(int partitionId, ServiceNamespace namespace, int replicaIndex);
}
