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

package com.hazelcast.sql.impl.exec.scan;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.collection.PartitionIdSet;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.ListRowBatch;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Executor for map scan.
 */
public class MapScanExec extends AbstractMapScanExec {
    /** Batch size. To be moved outside when the memory management is ready. */
    static final int BATCH_SIZE = 1024;

    private final MapContainer map;
    private final PartitionIdSet partitions;

    private int migrationStamp;
    private MapScanExecIterator recordIterator;

    private List<Row> currentRows;

    public MapScanExec(
        int id,
        MapContainer map,
        PartitionIdSet partitions,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        InternalSerializationService serializationService
    ) {
        super(id, map.getName(), keyDescriptor, valueDescriptor, fieldPaths, fieldTypes, projects, filter, serializationService);

        this.map = map;
        this.partitions = partitions;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        migrationStamp = map.getMapServiceContext().getService().getMigrationStamp();
        recordIterator = MapScanExecUtils.createIterator(map, partitions);
    }

    @Override
    public IterationResult advance0() {
        currentRows = null;

        while (recordIterator.tryAdvance()) {
            HeapRow row = prepareRow(recordIterator.getKey(), recordIterator.getValue());

            if (row != null) {
                if (currentRows == null) {
                    currentRows = new ArrayList<>(BATCH_SIZE);
                }

                currentRows.add(row);

                if (currentRows.size() == BATCH_SIZE) {
                    break;
                }
            }
        }

        boolean done = !recordIterator.hasNext();

        // Check for concurrent migration
        if (!map.getMapServiceContext().getService().validateMigrationStamp(migrationStamp)) {
            throw QueryException.error(SqlErrorCode.PARTITION_MIGRATED, "Map scan failed due to concurrent partition migration "
                + "(result consistency cannot be guaranteed)");
        }

        // Check for concurrent map destroy
        if (map.isDestroyed()) {
            throw QueryException.error(SqlErrorCode.MAP_DESTROYED, "IMap has been destroyed concurrently: " + mapName);
        }

        return done ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch0() {
        return currentRows != null ? new ListRowBatch(currentRows) : null;
    }

    @Override
    protected Extractors createExtractors() {
        return MapScanExecUtils.createExtractors(map);
    }

    public MapContainer getMap() {
        return map;
    }

    public PartitionIdSet getPartitions() {
        return partitions;
    }
}
