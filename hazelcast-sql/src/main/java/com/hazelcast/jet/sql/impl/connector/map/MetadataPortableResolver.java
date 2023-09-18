/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.connector.map;

import com.google.common.collect.ImmutableMap;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.internal.util.collection.DefaultedMap;
import com.hazelcast.jet.impl.util.ExceptionUtil;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.getSchemaId;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;

public final class MetadataPortableResolver implements KvMetadataResolver {
    public static final DefaultedMap<FieldType, QueryDataType> PORTABLE_TO_SQL = new DefaultedMap<>(
            new EnumMap<>(ImmutableMap.<FieldType, QueryDataType>builder()
                    .put(FieldType.BOOLEAN, QueryDataType.BOOLEAN)
                    .put(FieldType.BYTE, QueryDataType.TINYINT)
                    .put(FieldType.SHORT, QueryDataType.SMALLINT)
                    .put(FieldType.INT, QueryDataType.INT)
                    .put(FieldType.LONG, QueryDataType.BIGINT)
                    .put(FieldType.FLOAT, QueryDataType.REAL)
                    .put(FieldType.DOUBLE, QueryDataType.DOUBLE)
                    .put(FieldType.DECIMAL, QueryDataType.DECIMAL)
                    .put(FieldType.CHAR, QueryDataType.VARCHAR_CHARACTER)
                    .put(FieldType.UTF, QueryDataType.VARCHAR)
                    .put(FieldType.TIME, QueryDataType.TIME)
                    .put(FieldType.DATE, QueryDataType.DATE)
                    .put(FieldType.TIMESTAMP, QueryDataType.TIMESTAMP)
                    .put(FieldType.TIMESTAMP_WITH_TIMEZONE, QueryDataType.TIMESTAMP_WITH_TZ_OFFSET_DATE_TIME)
                    .put(FieldType.PORTABLE, QueryDataType.OBJECT)
                    .build()),
            QueryDataType.OBJECT);

    static final MetadataPortableResolver INSTANCE = new MetadataPortableResolver();

    private MetadataPortableResolver() { }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(PORTABLE_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(userFields, isKey);

        PortableId portableId = getSchemaId(fieldsByPath, PortableId::new, () -> portableId(options, isKey));
        ClassDefinition classDefinition = serializationService.getPortableContext()
                .lookupClassDefinition(portableId);

        return userFields.isEmpty()
                ? resolveFields(isKey, classDefinition)
                : resolveAndValidateFields(isKey, fieldsByPath, classDefinition);
    }

    private static Stream<MappingField> resolveFields(boolean isKey, ClassDefinition classDefinition) {
        if (classDefinition == null || classDefinition.getFieldCount() == 0) {
            // ClassDefinition does not exist, or it is empty, map the whole value
            String name = isKey ? KEY : VALUE;
            return Stream.of(new MappingField(name, QueryDataType.OBJECT, name));
        }

        return classDefinition.getFieldNames().stream()
                .map(name -> {
                    QueryPath path = new QueryPath(name, isKey);
                    QueryDataType type = PORTABLE_TO_SQL.getOrDefault(classDefinition.getFieldType(name));

                    return new MappingField(name, type, path.toString());
                });
    }

    private static Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            Map<QueryPath, MappingField> fieldsByPath,
            @Nullable ClassDefinition classDefinition
    ) {
        if (classDefinition != null) {
            for (String name : classDefinition.getFieldNames()) {
                final QueryPath path = new QueryPath(name, isKey);
                final QueryDataType type = PORTABLE_TO_SQL.getOrDefault(classDefinition.getFieldType(name));

                MappingField userField = fieldsByPath.get(path);
                if (userField != null && !type.getTypeFamily().equals(userField.type().getTypeFamily())) {
                    throw QueryException.error("Mismatch between declared and resolved type: " + userField.name());
                }
            }
        }
        return fieldsByPath.values().stream();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(resolvedFields, isKey);

        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        PortableId portableId = getSchemaId(fieldsByPath, PortableId::new, () -> portableId(options, isKey));
        ClassDefinition classDefinition = resolveClassDefinition(portableId, getFields(fieldsByPath),
                serializationService.getPortableContext());

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new PortableUpsertTargetDescriptor(classDefinition)
        );
    }

    @SuppressWarnings("ReturnCount")
    private static ClassDefinition resolveClassDefinition(PortableId portableId, Stream<Field> fields,
                                                          PortableContext context) {
        ClassDefinition classDefinition = context.lookupClassDefinition(portableId);
        if (classDefinition != null) {
            return classDefinition;
        }

        classDefinition = fields.reduce(new ClassDefinitionBuilder(portableId), (schema, field) -> {
            switch (field.type().getTypeFamily()) {
                case BOOLEAN:
                    return schema.addBooleanField(field.name());
                case TINYINT:
                    return schema.addByteField(field.name());
                case SMALLINT:
                    return schema.addShortField(field.name());
                case INTEGER:
                    return schema.addIntField(field.name());
                case BIGINT:
                    return schema.addLongField(field.name());
                case REAL:
                    return schema.addFloatField(field.name());
                case DOUBLE:
                    return schema.addDoubleField(field.name());
                case DECIMAL:
                    return schema.addDecimalField(field.name());
                case VARCHAR:
                    return schema.addStringField(field.name());
                case TIME:
                    return schema.addTimeField(field.name());
                case DATE:
                    return schema.addDateField(field.name());
                case TIMESTAMP:
                    return schema.addTimestampField(field.name());
                case TIMESTAMP_WITH_TIME_ZONE:
                    return schema.addTimestampWithTimezoneField(field.name());
                default:
                    if (field.type().isCustomType()) {
                        PortableId fieldId = new PortableId(field.type().getObjectTypeMetadata());
                        return schema.addPortableField(field.name(), resolveClassDefinition(
                                fieldId, field.type().getObjectFields().stream().map(Field::new), context));
                    } else {
                        throw QueryException.error("Unsupported type: " + field.type());
                    }
            }
        }, ExceptionUtil::notParallelizable).build();

        context.registerClassDefinition(classDefinition);
        return classDefinition;
    }

    public static PortableId portableId(Map<String, String> options, Boolean isKey) {
        String factoryIdProperty = isKey == null ? OPTION_TYPE_PORTABLE_FACTORY_ID :
                isKey ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID;
        String classIdProperty = isKey == null ? OPTION_TYPE_PORTABLE_CLASS_ID :
                isKey ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID;
        String versionProperty = isKey == null ? OPTION_TYPE_PORTABLE_CLASS_VERSION :
                isKey ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION;

        Integer factoryId = Optional.ofNullable(options.get(factoryIdProperty)).map(Integer::parseInt).orElse(null);
        Integer classId = Optional.ofNullable(options.get(classIdProperty)).map(Integer::parseInt).orElse(null);
        int version = Optional.ofNullable(options.get(versionProperty)).map(Integer::parseInt).orElse(0);

        if (factoryId == null || classId == null) {
            if (isKey != null) {
                throw QueryException.error(String.format("%s Portable ID (%s, %s and optional %s)"
                                + " is required to create Portable-based mapping", isKey ? "Key" : "Value",
                        factoryIdProperty, classIdProperty, versionProperty));
            }
            return null;
        }
        return new PortableId(factoryId, classId, version);
    }
}
