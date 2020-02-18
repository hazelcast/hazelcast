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

package com.hazelcast.sql.impl.exec;

import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.QueryFragmentContext;
import com.hazelcast.sql.impl.SqlUtils;
import com.hazelcast.sql.impl.expression.Expression;
import com.hazelcast.sql.impl.expression.KeyValueExtractorExpression;
import com.hazelcast.sql.impl.row.HeapRow;
import com.hazelcast.sql.impl.row.KeyValueRow;
import com.hazelcast.sql.impl.row.KeyValueRowExtractor;
import com.hazelcast.sql.impl.type.DataType;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.query.QueryConstants.KEY_ATTRIBUTE_NAME;
import static com.hazelcast.query.QueryConstants.THIS_ATTRIBUTE_NAME;

/**
 * Common operator for map scans.
 */
public abstract class AbstractMapScanExec extends AbstractExec implements KeyValueRowExtractor {
    /** Map name. */
    protected final String mapName;

    /** Field names. */
    protected final List<String> fieldNames;

    /** Field types. */
    protected final List<DataType> fieldTypes;

    /** Projects. */
    protected final List<Integer> projects;

    /** Filter. */
    protected final Expression<Boolean> filter;

    /** Serialization service. */
    protected InternalSerializationService serializationService;

    /** Extractors. */
    private Extractors extractors;

    /** Row to get data with extractors. */
    private KeyValueRow keyValueRow;

    protected AbstractMapScanExec(
        int id,
        String mapName,
        List<String> fieldNames,
        List<DataType> fieldTypes,
        List<Integer> projects,
        Expression<Boolean> filter
    ) {
        super(id);

        this.mapName = mapName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.projects = projects;
        this.filter = filter;
    }

    @Override
    protected final void setup0(QueryFragmentContext ctx) {
        serializationService = (InternalSerializationService) ctx.getNodeEngine().getSerializationService();
        extractors = createExtractors();

        List<KeyValueExtractorExpression<?>> fieldExpressions = new ArrayList<>(fieldNames.size());

        for (int i = 0; i < fieldNames.size(); i++) {
            String fieldName = fieldNames.get(i);
            DataType fieldType = fieldTypes.get(i);

            fieldExpressions.add(new KeyValueExtractorExpression<>(fieldName, fieldType));
        }

        keyValueRow = new KeyValueRow(this, fieldExpressions);
    }

    @Override
    public boolean canReset() {
        return true;
    }

    @Override
    public Object extract(Object key, Object val, String path) {
        Object res;

        if (KEY_ATTRIBUTE_NAME.value().equals(path)) {
            res = key;
        } else if (THIS_ATTRIBUTE_NAME.value().equals(path)) {
            res = val;
        } else {
            String keyPath = SqlUtils.extractKeyPath(path);

            Object target;

            if (keyPath != null) {
                target = key;

                path = keyPath;
            } else {
                target = val;
            }

            res = extractors.extract(target, path, null);
        }

        if (res instanceof HazelcastJsonValue) {
            res = Json.parse(res.toString());
        }

        return res;
    }

    protected HeapRow prepareRow(Object key, Object val) {
        keyValueRow.setKeyValue(key, val);

        // Filter.
        if (filter != null && !filter.eval(keyValueRow)) {
            return null;
        }

        // Project.
        HeapRow row = new HeapRow(projects.size());

        for (int j = 0; j < projects.size(); j++) {
            Object projectRes = keyValueRow.getColumn(projects.get(j));

            row.set(j, projectRes);
        }

        return row;
    }

    /**
     * Create extractors for the given operator.
     *
     * @return Extractors for map.
     */
    protected abstract Extractors createExtractors();
}
