/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Address;

public final class ReplicaSyncInfo {
    final int partitionId;
    final int replicaIndex;
    final Address target;

    ReplicaSyncInfo(int partitionId, int replicaIndex, Address target) {
        this.partitionId = partitionId;
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

        ReplicaSyncInfo that = (ReplicaSyncInfo) o;
        if (partitionId != that.partitionId) {
            return false;
        }
        if (replicaIndex != that.replicaIndex) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + replicaIndex;
        return result;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{partitionId=" + partitionId + ", replicaIndex=" + replicaIndex + ", target="
                + target + '}';
    }
}
