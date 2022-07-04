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

import com.hazelcast.internal.services.ServiceNamespace;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

// read and updated only by partition threads
final class PartitionReplicaVersions {
    private final int partitionId;

    private final Map<ServiceNamespace, PartitionReplicaFragmentVersions> fragmentVersionsMap = new HashMap<>();

    PartitionReplicaVersions(int partitionId) {
        this.partitionId = partitionId;
    }

    long[] incrementAndGet(ServiceNamespace namespace, int backupCount) {
        return getFragmentVersions(namespace).incrementAndGet(backupCount);
    }

    long[] get(ServiceNamespace namespace) {
        return getFragmentVersions(namespace).get();
    }

    /**
     * Returns whether given replica version is behind the current version or not.
     * @param namespace replica namespace
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return true if given version is stale, false otherwise
     */
    boolean isStale(ServiceNamespace namespace, long[] newVersions, int replicaIndex) {
        return getFragmentVersions(namespace).isStale(newVersions, replicaIndex);
    }

    /**
     * Updates replica version if it is newer than current version. Otherwise has no effect.
     * Marks versions as dirty if version increase is not incremental.
     *
     * @param namespace replica namespace
     * @param newVersions new replica versions
     * @param replicaIndex replica index
     * @return returns false if versions are dirty, true otherwise
     */
    boolean update(ServiceNamespace namespace, long[] newVersions, int replicaIndex) {
        return getFragmentVersions(namespace).update(newVersions, replicaIndex);
    }

    void set(ServiceNamespace namespace, long[] newVersions, int fromReplica) {
        getFragmentVersions(namespace).set(newVersions, fromReplica);
    }

    boolean isDirty(ServiceNamespace namespace) {
        return getFragmentVersions(namespace).isDirty();
    }

    void clear(ServiceNamespace namespace) {
        getFragmentVersions(namespace).clear();
    }

    private PartitionReplicaFragmentVersions getFragmentVersions(ServiceNamespace namespace) {
        PartitionReplicaFragmentVersions fragmentVersions = fragmentVersionsMap.get(namespace);
        if (fragmentVersions == null) {
            fragmentVersions = new PartitionReplicaFragmentVersions(partitionId, namespace);
            fragmentVersionsMap.put(namespace, fragmentVersions);
        }
        return fragmentVersions;
    }

    void retainNamespaces(Collection<ServiceNamespace> namespaces) {
        fragmentVersionsMap.keySet().retainAll(namespaces);
    }

    Collection<ServiceNamespace> getNamespaces() {
        return Collections.unmodifiableCollection(fragmentVersionsMap.keySet());
    }

    @Override
    public String toString() {
        return "PartitionReplicaVersions{" + "partitionId=" + partitionId + ", fragmentVersions=" + fragmentVersionsMap
                + '}';
    }
}
