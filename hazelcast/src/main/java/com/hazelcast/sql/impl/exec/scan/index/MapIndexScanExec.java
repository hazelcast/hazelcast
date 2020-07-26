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

package com.hazelcast.sql.impl.exec.scan.index;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Index scan executor.
 */
public class MapIndexScanExec extends MapScanExec {

    private final String indexName;
    private final IndexFilter indexFilter;
    private final List<QueryDataType> converterTypes;
    private final int componentCount;

    @SuppressWarnings("checkstyle:ParameterNumber")
    public MapIndexScanExec(
        int id,
        MapContainer map,
        PartitionIdSet parts,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        InternalSerializationService serializationService,
        String indexName,
        int componentCount,
        IndexFilter indexFilter,
        List<QueryDataType> converterTypes
    ) {
        // TODO: How to deal with passed partitions? Should we check that they are still owned during setup?
        super(
            id,
            map,
            parts,
            keyDescriptor,
            valueDescriptor,
            fieldPaths,
            fieldTypes,
            projects,
            filter,
            serializationService
        );

        this.indexName = indexName;
        this.componentCount = componentCount;
        this.indexFilter = indexFilter;
        this.converterTypes = converterTypes;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    protected KeyValueIterator createIterator() {
        return new MapIndexScanExecIterator(map, indexName, componentCount, indexFilter, converterTypes, ctx);
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        // TODO: Move this into iterator (see IndexImpl.indexedPartitions)
        // Check if expected partitions are owned by this member. Future migrations are tracked via migration stamp.
        PartitionIdSet owned = map.getMapServiceContext().getOwnedPartitions();

        if (owned.containsAll(partitions)) {
            return;
        }

        List<Integer> missedPartitions = new ArrayList<>();

        for (int partition : partitions) {
            boolean isOwned = map.getMapServiceContext().getOwnedPartitions().contains(partition);

            if (!isOwned) {
                missedPartitions.add(partition);
            }
        }

        if (!partitions.isEmpty()) {
            throw QueryException.error(SqlErrorCode.PARTITION_MIGRATED,
                "Partitions are not owned by member: " + missedPartitions);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldPaths=" + fieldPaths + ", projects=" + projects
            + "indexName=" + indexName + ", indexFilter=" + indexFilter + ", remainderFilter=" + filter
            + ", partitionCount=" + partitions.size() + '}';
    }
}
