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

package com.hazelcast.sql.impl.row;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.query.impl.getters.Extractors;
import com.hazelcast.sql.impl.extract.QueryExtractor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTarget;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.List;

/**
 * Key-value row. Appears during iteration over a data stored in map or it's index.
 */
public final class KeyValueRow implements Row {

    private final QueryTarget keyTarget;
    private final QueryTarget valueTarget;
    private final QueryExtractor[] fieldExtractors;

    private KeyValueRow(QueryTarget keyTarget, QueryTarget valueTarget, QueryExtractor[] fieldExtractors) {
        this.keyTarget = keyTarget;
        this.valueTarget = valueTarget;
        this.fieldExtractors = fieldExtractors;
    }

    public static KeyValueRow create(
        QueryTargetDescriptor keyDescriptor,
        QueryTargetDescriptor valueDescriptor,
        List<String> fieldPaths,
        List<QueryDataType> fieldTypes,
        Extractors extractors,
        InternalSerializationService serializationService
    ) {
        QueryTarget keyTarget = keyDescriptor.create(serializationService, extractors, true);
        QueryTarget valueTarget = valueDescriptor.create(serializationService, extractors, false);

        QueryExtractor[] fieldExtractors = new QueryExtractor[fieldPaths.size()];

        for (int i = 0; i < fieldPaths.size(); i++) {
            String fieldPath = fieldPaths.get(i);
            QueryDataType fieldType = fieldTypes.get(i);

            fieldExtractors[i] = createExtractor(keyTarget, valueTarget, fieldPath, fieldType);
        }

        return new KeyValueRow(keyTarget, valueTarget, fieldExtractors);
    }

    public void setKeyValue(Object rawKey, Object rawValue) {
        keyTarget.setTarget(rawKey);
        valueTarget.setTarget(rawValue);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T get(int idx) {
        return (T) fieldExtractors[idx].get();
    }

    @Override
    public int getColumnCount() {
        return fieldExtractors.length;
    }

    private static QueryExtractor createExtractor(
        QueryTarget keyTarget,
        QueryTarget valueTarget,
        String path,
        QueryDataType type
    ) {
        QueryPath path0 = QueryPath.create(path);

        QueryTarget target = path0.isKey() ? keyTarget : valueTarget;

        return target.createExtractor(path0.getPath(), type);
    }
}
