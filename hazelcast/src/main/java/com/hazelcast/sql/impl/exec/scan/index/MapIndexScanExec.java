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
import com.hazelcast.query.impl.GlobalIndexPartitionTracker;
import com.hazelcast.query.impl.Indexes;
import com.hazelcast.query.impl.InternalIndex;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.scan.KeyValueIterator;
import com.hazelcast.sql.impl.exec.scan.MapScanExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Index scan executor.
 */
public class MapIndexScanExec extends MapScanExec {

    private final String indexName;
    private final IndexFilter indexFilter;
    private final List<QueryDataType> converterTypes;
    private final int componentCount;

    private InternalIndex index;

    /** Stamp to ensure that indexed partitions are stable throughout query execution. */
    private Long partitionStamp;

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

    @Override
    protected KeyValueIterator createIterator() {
        // Find the index
        Indexes indexes = map.getIndexes();

        if (indexes == null) {
            throw QueryException.error(
                SqlErrorCode.INDEX_INVALID,
                "Cannot use the index \"" + indexName + "\" of the IMap \"" + map.getName() + "\" because it is not global "
                    + "(make sure the property \"" + ClusterProperty.GLOBAL_HD_INDEX_ENABLED + "\" is set to \"true\")"
            ).markInvalidate();
        }

        index = indexes.getIndex(indexName);

        if (index == null) {
            throw QueryException.error(
                SqlErrorCode.INDEX_INVALID,
                "Cannot use the index \"" + indexName + "\" of the IMap \"" + map.getName() + "\" because it doesn't exist"
            ).markInvalidate();
        }

        // Make sure that required partitions are indexed
        partitionStamp = index.getPartitionStamp(partitions);

        if (partitionStamp == GlobalIndexPartitionTracker.STAMP_INVALID) {
            throw invalidIndexStamp();
        }

        return new MapIndexScanExecIterator(mapName, index, componentCount, indexFilter, converterTypes, ctx);
    }

    @Override
    protected void validateConsistency() {
        if (!index.validatePartitionStamp(partitionStamp)) {
            throw invalidIndexStamp();
        }
    }

    private QueryException invalidIndexStamp() {
        throw QueryException.error(
            SqlErrorCode.INDEX_INVALID,
            "Cannot use the index \"" + indexName + "\" of the IMap \"" + mapName + "\" due to concurrent migration, "
                + "or because index creation is still in progress"
        ).markInvalidate();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldPaths=" + fieldPaths + ", projects=" + projects
            + "indexName=" + indexName + ", indexFilter=" + indexFilter + ", remainderFilter=" + filter
            + ", partitionCount=" + partitions.size() + '}';
    }
}
