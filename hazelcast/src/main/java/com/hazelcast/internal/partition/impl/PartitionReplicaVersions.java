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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.spi.ReplicaFragmentAwareService.ReplicaFragmentNamespace;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

// read and updated only by partition threads
final class PartitionReplicaVersions {
    private final int partitionId;

    private final Map<ReplicaFragmentNamespace, PartitionReplicaFragmentVersions> fragmentVersionsMap
            = new HashMap<ReplicaFragmentNamespace, PartitionReplicaFragmentVersions>();

    PartitionReplicaVersions(int partitionId) {
        this.partitionId = partitionId;
    }

    long[] incrementAndGet(ReplicaFragmentNamespace namespace, int backupCount) {
        return getFragmentVersions(namespace).incrementAndGet(backupCount);
    }

    long[] get(ReplicaFragmentNamespace namespace) {
        return getFragmentVersions(namespace).get();
    }

    /**
     * Returns whether given replica version is behind the current version or not.
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return true if given version is stale, false otherwise
     */
    boolean isStale(ReplicaFragmentNamespace namespace, long[] newVersions, int replicaIndex) {
        return getFragmentVersions(namespace).isStale(newVersions, replicaIndex);
    }

    /**
     * Updates replica version if it is newer than current version. Otherwise has no effect.
     * Marks versions as dirty if version increase is not incremental.
     *
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return returns false if versions are dirty, true otherwise
     */
    boolean update(ReplicaFragmentNamespace namespace, long[] newVersions, int replicaIndex) {
        return getFragmentVersions(namespace).update(newVersions, replicaIndex);
    }

    void set(ReplicaFragmentNamespace namespace, long[] newVersions, int fromReplica) {
        getFragmentVersions(namespace).set(newVersions, fromReplica);
    }

    boolean isDirty(ReplicaFragmentNamespace namespace) {
        return getFragmentVersions(namespace).isDirty();
    }

    void clear(ReplicaFragmentNamespace namespace) {
        getFragmentVersions(namespace).clear();
    }

    private PartitionReplicaFragmentVersions getFragmentVersions(ReplicaFragmentNamespace namespace) {
        PartitionReplicaFragmentVersions fragmentVersions = fragmentVersionsMap.get(namespace);
        if (fragmentVersions == null) {
            fragmentVersions = new PartitionReplicaFragmentVersions(partitionId, namespace);
            fragmentVersionsMap.put(namespace, fragmentVersions);
        }
        return fragmentVersions;
    }

    void retainNamespaces(Set<ReplicaFragmentNamespace> namespaces) {
        fragmentVersionsMap.keySet().retainAll(namespaces);
    }

    Collection<ReplicaFragmentNamespace> getNamespaces() {
        return fragmentVersionsMap.keySet();
    }

    @Override
    public String toString() {
        return "PartitionReplicaVersions{" + "partitionId=" + partitionId + ", fragmentVersions=" + fragmentVersionsMap
                + '}';
    }
}
