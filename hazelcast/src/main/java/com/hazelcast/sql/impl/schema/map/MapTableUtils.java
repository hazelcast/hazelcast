/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;

/**
 * Utility methods for schema resolution.
 */
public final class MapTableUtils {
    private MapTableUtils() {
        // No-op.
    }

    public static long estimatePartitionedMapRowCount(NodeEngine nodeEngine, MapServiceContext context, String mapName) {
        long entryCount = 0L;

        PartitionIdSet ownerPartitions = context.getOwnedPartitions();

        for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
            if (!ownerPartitions.contains(partitionContainer.getPartitionId())) {
                continue;
            }

            RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(mapName);

            if (recordStore == null) {
                continue;
            }

            entryCount += recordStore.size();
        }

        int memberCount = nodeEngine.getClusterService().getMembers(MemberSelectors.DATA_MEMBER_SELECTOR).size();

        return entryCount * memberCount;
    }
}
