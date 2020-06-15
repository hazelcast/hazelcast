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

package com.hazelcast.sql.impl.schema.map.options;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_FACTORY_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_FACTORY_ID;

// TODO: deduplicate with MapSampleMetadataResolver
public class PortableMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        // TODO: ensure binary ???
        Integer factoryId = parseInt(options.get(isKey ? TO_KEY_FACTORY_ID : TO_VALUE_FACTORY_ID));
        Integer classId = parseInt(options.get(isKey ? TO_KEY_CLASS_ID : TO_VALUE_CLASS_ID));
        Integer classVersion = parseInt(options.get(isKey ? TO_KEY_CLASS_VERSION : TO_VALUE_CLASS_VERSION));
        if (factoryId != null && classId != null && classVersion != null) {
            ClassDefinition classDefinition = serializationService
                    .getPortableContext()
                    .lookupClassDefinition(factoryId, classId, classVersion);
            return resolvePortable(classDefinition, isKey);
        }
        return null;
    }

    private static Integer parseInt(String value) {
        if (value == null) {
            return null;
        }

        try {
            return Integer.valueOf(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static MapOptionsMetadata resolvePortable(ClassDefinition clazz, boolean isKey) {
        TreeMap<String, TableField> fields = new TreeMap<>();

        // Add regular fields.
        for (String name : clazz.getFieldNames()) {
            FieldType portableType = clazz.getFieldType(name);

            QueryDataType type = resolvePortableType(portableType);

            fields.putIfAbsent(name, new MapTableField(name, type, false, new QueryPath(name, isKey)));
        }

        // Add top-level object.
        String topName = isKey ? QueryPath.KEY : QueryPath.VALUE;
        QueryPath topPath = isKey ? QueryPath.KEY_PATH : QueryPath.VALUE_PATH;
        fields.put(topName, new MapTableField(topName, QueryDataType.OBJECT, !fields.isEmpty(), topPath));

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PortableUpsertTargetDescriptor(clazz.getFactoryId(), clazz.getClassId(), clazz.getVersion()),
                new LinkedHashMap<>(fields)
        );
    }

    @SuppressWarnings("checkstyle:ReturnCount")
    private static QueryDataType resolvePortableType(FieldType portableType) {
        switch (portableType) {
            case BOOLEAN:
                return QueryDataType.BOOLEAN;
            case BYTE:
                return QueryDataType.TINYINT;
            case SHORT:
                return QueryDataType.SMALLINT;
            case CHAR:
                return QueryDataType.VARCHAR_CHARACTER;
            case UTF:
                return QueryDataType.VARCHAR;
            case INT:
                return QueryDataType.INT;
            case LONG:
                return QueryDataType.BIGINT;
            case FLOAT:
                return QueryDataType.REAL;
            case DOUBLE:
                return QueryDataType.DOUBLE;
            default:
                return QueryDataType.OBJECT;
        }
    }
}
