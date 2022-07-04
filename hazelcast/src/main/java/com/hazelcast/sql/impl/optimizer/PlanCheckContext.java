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

package com.hazelcast.sql.impl.optimizer;

import com.hazelcast.internal.util.collection.PartitionIdSet;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Context that is passed to plans to check whether the plan is still valid.
 */
public class PlanCheckContext {
    /** Unique IDs of resolved objects. */
    private final Set<PlanObjectKey> objectKeys;

    /** Current distribution of partitions. */
    private final Map<UUID, PartitionIdSet> partitions;

    public PlanCheckContext(Set<PlanObjectKey> objectKeys, Map<UUID, PartitionIdSet> partitions) {
        this.objectKeys = objectKeys;
        this.partitions = partitions;
    }

    public boolean isValid(Set<PlanObjectKey> expectedObjectVersions) {
        // If some of objects used in the plan have changed, then the plan should be re-created.
        // Examples are index creation, map destroy, external object redefinition.
        return objectKeys.containsAll(expectedObjectVersions);
    }

    public boolean isValid(Set<PlanObjectKey> expectedObjectVersions, Map<UUID, PartitionIdSet> expectedPartitions) {
        // If some of objects used in the plan have changed, then the plan should be re-created.
        // Examples are index creation, map destroy, external object redefinition.
        if (!objectKeys.containsAll(expectedObjectVersions)) {
            return false;
        }

        // Plans are created for specific partitions. If distribution changes, the plan can not be used anymore.
        return partitions.equals(expectedPartitions);
    }
}
