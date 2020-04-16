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
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.sql.impl.exec.IterationResult;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.row.RowBatch;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Executor for map scan.
 */
@SuppressWarnings("rawtypes")
public class ReplicatedMapScanExec extends AbstractMapScanExec {
    /** Map. */
    private final ReplicatedMapProxy map;

    /** All rows fetched on first access. */
    private Collection<Row> rows;

    /** Iterator over rows. */
    private Iterator<Row> rowsIter;

    /** Current row. */
    private Row currentRow;

    public ReplicatedMapScanExec(
        int id,
        ReplicatedMapProxy map,
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
    }

    @SuppressWarnings("rawtypes")
    @Override
    public IterationResult advance0() {
        if (rows == null) {
            rows = new ArrayList<>();

            ReplicatedMapService svc = (ReplicatedMapService) map.getService();

            Collection<ReplicatedRecordStore> stores = svc.getAllReplicatedRecordStores(mapName);

            for (ReplicatedRecordStore store : stores) {
                Iterator<ReplicatedRecord> iter = store.recordIterator();

                while (iter.hasNext()) {
                    ReplicatedRecord record = iter.next();

                    HeapRow row = prepareRow(record.getKey(), record.getValue());

                    if (row != null) {
                        rows.add(row);
                    }
                }
            }

            rowsIter = rows.iterator();
        }

        if (rowsIter.hasNext()) {
            currentRow = rowsIter.next();

            return IterationResult.FETCHED;
        } else {
            currentRow = null;

            return IterationResult.FETCHED_DONE;
        }
    }

    @Override
    public RowBatch currentBatch0() {
        return currentRow;
    }

    @Override
    protected void reset0() {
        rows = null;
        rowsIter = null;
        currentRow = null;
    }

    @Override
    protected Extractors createExtractors() {
        return map.getExtractors();
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{mapName=" + mapName + ", fieldNames=" + fieldNames
            + ", projects=" + projects + ", filter=" + filter + '}';
    }
}
