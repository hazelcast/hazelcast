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
import com.hazelcast.sql.impl.exec.AbstractExec;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.row.EmptyRow;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.Row;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.worker.QueryFragmentContext;

import java.util.List;

/**
 * Common operator for map scans.
 */
public abstract class AbstractMapScanExec extends AbstractExec {

    protected final String mapName;
    protected final QueryTargetDescriptor keyDescriptor;
    protected final QueryTargetDescriptor valueDescriptor;
    protected final List<QueryPath> fieldPaths;
    protected final List<QueryDataType> fieldTypes;
    protected final List<Integer> projects;
    protected final Expression<Boolean> filter;
    private final InternalSerializationService serializationService;
    private MapScanRow row;

    protected AbstractMapScanExec(
        int id,
        String mapName,
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<QueryPath> fieldPaths,
        List<QueryDataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter,
        InternalSerializationService serializationService
    ) {
        super(id);

        this.mapName = mapName;
        this.keyDescriptor = keyDescriptor;
        this.valueDescriptor = valueDescriptor;
        this.fieldPaths = fieldPaths;
        this.fieldTypes = fieldTypes;
        this.projects = projects;
        this.filter = filter;
        this.serializationService = serializationService;
    }

    @Override
    protected final void setup0(QueryFragmentContext ctx) {
        row = MapScanRow.create(
            keyDescriptor,
            valueDescriptor,
            fieldPaths,
            fieldTypes,
            createExtractors(),
            serializationService
        );

        setup1(ctx);
    }

    protected void setup1(QueryFragmentContext ctx) {
        // No-op.
    }

    /**
     * Prepare the row for the given key and value:
     * 1) Check filter
     * 2) Extract projections
     *
     * @param rawkey Key (data or object)
     * @param rawValue Value (data or object)
     * @return Row that is ready for processing by parent operators or {@code null} if the row hasn't passed the filter.
     */
    protected Row prepareRow(Object rawkey, Object rawValue) {
        row.setKeyValue(rawkey, rawValue);

        // Filter.
        if (filter != null && !filter.eval(row, ctx)) {
            return null;
        }

        // Project.
        if (projects.size() == 0) {
            return EmptyRow.INSTANCE;
        }

        HeapRow row = new HeapRow(projects.size());

        for (int j = 0; j < projects.size(); j++) {
            Object projectRes = this.row.get(projects.get(j));

            row.set(j, projectRes);
        }

        return row;
    }

    /**
     * Create extractors for the given operator.
     *
     * @return Extractors.
     */
    protected abstract Extractors createExtractors();

    public QueryTargetDescriptor getKeyDescriptor() {
        return keyDescriptor;
    }

    public QueryTargetDescriptor getValueDescriptor() {
        return valueDescriptor;
    }

    public List<QueryPath> getFieldPaths() {
        return fieldPaths;
    }

    public List<QueryDataType> getFieldTypes() {
        return fieldTypes;
    }

    public List<Integer> getProjects() {
        return projects;
    }

    public Expression<Boolean> getFilter() {
        return filter;
    }
}
