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
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
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
    /** Batch size. */
    private static final int BATCH_SIZE = 1000;

    /** Underlying map. */
    private final MapContainer map;

    /** Partitions to be scanned. */
    private final PartitionIdSet parts;

    /** Records iterator. */
    private MapScanExecIterator recordIterator;

    /** Current row. */
    private List<Row> currentBatch;

    public MapScanExec(
        int id,
        MapContainer map,
        PartitionIdSet parts,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<String> fieldNames,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        InternalSerializationService serializationService
    ) {
        super(id, map.getName(), keyDescriptor, valueDescriptor, fieldNames, fieldTypes, projects, filter, serializationService);

        this.map = map;
        this.parts = parts;
    }

    @Override
    protected void setup1(QueryFragmentContext ctx) {
        recordIterator = MapScanExecUtils.createIterator(map, parts);
    }

    @Override
    public IterationResult advance0() {
        currentBatch = null;

        while (recordIterator.tryAdvance()) {
            HeapRow row = prepareRow(recordIterator.getKey(), recordIterator.getValue());

            if (row != null) {
                if (currentBatch == null) {
                    currentBatch = new ArrayList<>(BATCH_SIZE);
                }

                currentBatch.add(row);

                if (currentBatch.size() == BATCH_SIZE) {
                    break;
                }
            }
        }

        boolean done = currentBatch == null || !recordIterator.canAdvance();

        return done ? IterationResult.FETCHED_DONE : IterationResult.FETCHED;
    }

    @Override
    public RowBatch currentBatch0() {
        return currentBatch != null ? new ListRowBatch(currentBatch) : null;
    }

    @Override
    protected void reset0() {
        recordIterator = null;
        currentBatch = null;
    }

    @Override
    protected Extractors createExtractors() {
        return MapScanExecUtils.createExtractors(map);
    }

    public MapContainer getMap() {
        return map;
    }

    public PartitionIdSet getParts() {
        return parts;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldNames=" + fieldNames
            + ", projects=" + projects + ", filter=" + filter + ", partitionCount=" + parts.size() + '}';
    }
}
