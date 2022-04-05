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

package com.hazelcast.wan;

import com.hazelcast.internal.partition.PartitionMigrationEvent;
import com.hazelcast.internal.partition.PartitionReplicationEvent;
import com.hazelcast.internal.services.ServiceNamespace;

import java.util.Collection;
import java.util.Set;

/**
 * Interface for WAN publisher migration related events. Can be implemented
 * by WAN publishers to listen to migration events, for example to maintain
 * the WAN event counters.
 * <p>
 * None of the methods of this interface is expected to block or fail.
 *
 * @param <T> WAN event container type (used for replication and migration inside the
 *            cluster)
 * @see PartitionMigrationEvent
 * @see com.hazelcast.internal.partition.MigrationAwareService
 */
public interface WanMigrationAwarePublisher<T> {
    /**
     * Indicates that migration started for a given partition
     *
     * @param event the migration event
     */
    void onMigrationStart(PartitionMigrationEvent event);

    /**
     * Indicates that migration is committing for a given partition
     *
     * @param event the migration event
     */
    void onMigrationCommit(PartitionMigrationEvent event);

    /**
     * Indicates that migration is rolling back for a given partition
     *
     * @param event the migration event
     */
    void onMigrationRollback(PartitionMigrationEvent event);

    /**
     * Returns a container containing the WAN events for the given replication
     * {@code event} and {@code namespaces} to be replicated. The replication
     * here refers to the intra-cluster replication between members in a single
     * cluster and does not refer to WAN replication, e.g. between two clusters.
     * Invoked when migrating WAN replication data between members in a cluster.
     *
     * @param event      the replication event
     * @param namespaces namespaces which will be replicated
     * @return the WAN event container
     * @see #processEventContainerReplicationData(int, Object)
     */
    default T prepareEventContainerReplicationData(PartitionReplicationEvent event,
                                                   Collection<ServiceNamespace> namespaces) {
        return null;
    }

    /**
     * Processes the WAN event container received through intra-cluster replication
     * or migration. This method may completely remove existing WAN events for
     * the given {@code partitionId} or it may append the given
     * {@code eventContainer} to the existing events.
     * Invoked when migrating WAN replication data between members in a cluster.
     *
     * @param partitionId    partition ID which is being replicated or migrated
     * @param eventContainer the WAN event container
     * @see #prepareEventContainerReplicationData(PartitionReplicationEvent, Collection)
     */
    default void processEventContainerReplicationData(int partitionId, T eventContainer) {
    }

    /**
     * Collect the namespaces of all WAN event containers that should be replicated
     * by the replication event.
     * Invoked when migrating WAN replication data between members in a cluster.
     *
     * @param event      the replication event
     * @param namespaces the set in which namespaces should be added
     */
    default void collectAllServiceNamespaces(PartitionReplicationEvent event,
                                             Set<ServiceNamespace> namespaces) {
    }
}
