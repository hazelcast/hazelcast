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

package com.hazelcast.internal.partition;

import com.hazelcast.internal.services.ServiceNamespace;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PartitionReplicaVersionsView {
    private final Map<ServiceNamespace, long[]> versions;
    private final Set<ServiceNamespace> dirtyNamespaces;

    PartitionReplicaVersionsView(Map<ServiceNamespace, long[]> replicaVersions, Set<ServiceNamespace> dirtyNamespaces) {
        Assert.assertNotNull(replicaVersions);
        Assert.assertNotNull(dirtyNamespaces);
        this.versions = replicaVersions;
        this.dirtyNamespaces = dirtyNamespaces;
    }

    public Collection<ServiceNamespace> getNamespaces() {
        return versions.keySet();
    }

    public long[] getVersions(ServiceNamespace ns) {
        return versions.get(ns);
    }

    public boolean isDirty(ServiceNamespace ns) {
        return dirtyNamespaces.contains(ns);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof PartitionReplicaVersionsView)) {
            return false;
        }

        PartitionReplicaVersionsView that = (PartitionReplicaVersionsView) o;

        long[] emptyVersions = new long[InternalPartition.MAX_BACKUP_COUNT];

        Collection<ServiceNamespace> namespaces = new HashSet<ServiceNamespace>(versions.keySet());
        namespaces.addAll(that.versions.keySet());

        for (ServiceNamespace ns : namespaces) {
            long[] thisVersions = getVersions(ns);
            long[] thatVersions = that.getVersions(ns);

            if (thisVersions == null) {
                thisVersions = emptyVersions;
            }
            if (thatVersions == null) {
                thatVersions = emptyVersions;
            }

            if (!Arrays.equals(thisVersions, thatVersions)) {
                return false;
            }
        }

        if (dirtyNamespaces.size() != that.dirtyNamespaces.size()) {
            return false;
        }

        for (ServiceNamespace ns : dirtyNamespaces) {
            if (!that.isDirty(ns)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = versions.hashCode();
        result = 31 * result + dirtyNamespaces.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Replica Versions={");
        for (Map.Entry<ServiceNamespace, long[]> entry : versions.entrySet()) {
            s.append(entry.getKey()).append("=").append(Arrays.toString(entry.getValue())).append(",");
        }
        s.append("}, Dirty Namespaces=").append(dirtyNamespaces);
        return s.toString();
    }
}
