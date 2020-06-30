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
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.sql.impl.QueryException;
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
import java.util.Map.Entry;

import static com.hazelcast.sql.impl.connector.SqlConnector.PORTABLE_SERIALIZATION_FORMAT;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_KEY_FACTORY_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_ID;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_CLASS_VERSION;
import static com.hazelcast.sql.impl.connector.SqlKeyValueConnector.TO_VALUE_FACTORY_ID;
import static java.lang.String.format;

// TODO: deduplicate with MapSampleMetadataResolver
public final class PortableMapOptionsMetadataResolver implements MapOptionsMetadataResolver {

    public static final PortableMapOptionsMetadataResolver INSTANCE = new PortableMapOptionsMetadataResolver();

    private PortableMapOptionsMetadataResolver() {
    }

    @Override
    public String supportedFormat() {
        return PORTABLE_SERIALIZATION_FORMAT;
    }

    @Override
    public MapOptionsMetadata resolve(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean isKey,
            InternalSerializationService serializationService
    ) {
        String factoryIdProperty = isKey ? TO_KEY_FACTORY_ID : TO_VALUE_FACTORY_ID;
        String factoryId = options.get(factoryIdProperty);
        String classIdProperty = isKey ? TO_KEY_CLASS_ID : TO_VALUE_CLASS_ID;
        String classId = options.get(classIdProperty);
        String classVersionProperty = isKey ? TO_KEY_CLASS_VERSION : TO_VALUE_CLASS_VERSION;
        String classVersion = options.get(classVersionProperty);

        if (factoryId == null || classId == null || classVersion == null) {
            throw QueryException.error(
                    format("Unable to resolve table metadata. Missing ['%s'|'%s'|'%s'] option(s)",
                            factoryIdProperty, classIdProperty, classVersionProperty));
        }

        ClassDefinition classDefinition = lookupClassDefinition(
                serializationService,
                Integer.parseInt(factoryId),
                Integer.parseInt(classId),
                Integer.parseInt(classVersion)
        );
        return resolvePortable(externalFields, classDefinition, isKey);
    }

    // TODO: extract to util class ???
    public static ClassDefinition lookupClassDefinition(
            InternalSerializationService serializationService,
            int factoryId,
            int classId,
            int classVersion
    ) {
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(factoryId, classId, classVersion);
        if (classDefinition == null) {
            throw QueryException.dataException(
                    format("Unable to find class definition for factoryId: %s, classId: %s, classVersion: %s",
                            factoryId, classId, classVersion)
            );
        }
        return classDefinition;
    }

    private MapOptionsMetadata resolvePortable(
            List<ExternalField> externalFields,
            ClassDefinition classDefinition,
            boolean isKey
    ) {
        Map<QueryPath, ExternalField> externalFieldsByPath =
                extractFields(externalFields, isKey, name -> new QueryPath(name, false));

        LinkedHashMap<String, TableField> fields = new LinkedHashMap<>();

        for (Entry<String, FieldType> entry : resolvePortable(classDefinition).entrySet()) {
            QueryPath path = new QueryPath(entry.getKey(), isKey);
            QueryDataType type = resolvePortableType(entry.getValue());

            ExternalField externalField = externalFieldsByPath.get(path);
            if (externalField != null && !externalField.type().equals(type)) {
                throw QueryException.error(
                        format("Mismatch between declared and inferred type - '%s'", externalField.name())
                );
            }
            String name = externalField == null ? entry.getKey() : externalField.name();

            TableField field = new MapTableField(name, type, false, path);

            fields.putIfAbsent(field.getName(), field);
        }

        for (Entry<QueryPath, ExternalField> entry : externalFieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            String name = entry.getValue().name();
            QueryDataType type = entry.getValue().type();

            TableField field = new MapTableField(name, type, false, path);

            fields.put(field.getName(), field);
        }

        return new MapOptionsMetadata(
                GenericQueryTargetDescriptor.INSTANCE,
                new PortableUpsertTargetDescriptor(
                        classDefinition.getFactoryId(),
                        classDefinition.getClassId(),
                        classDefinition.getVersion()
                ),
                fields
        );
    }

    private static Map<String, FieldType> resolvePortable(ClassDefinition classDefinition) {
        Map<String, FieldType> fields = new LinkedHashMap<>();
        for (int i = 0; i < classDefinition.getFieldCount(); i++) {
            FieldDefinition fieldDefinition = classDefinition.getField(i);
            fields.putIfAbsent(fieldDefinition.getName(), fieldDefinition.getType());
        }
        return fields;
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
