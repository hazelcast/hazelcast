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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.jet.sql.impl.inject.PortableUpsertTargetDescriptor;
import com.hazelcast.jet.sql.impl.schema.TypesUtils;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataTypeFamily;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnectorUtil.asInt;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.maybeAddDefaultField;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;

final class MetadataPortableResolver implements KvMetadataResolver {

    static final MetadataPortableResolver INSTANCE = new MetadataPortableResolver();

    private MetadataPortableResolver() {
    }

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
        Map<QueryPath, MappingField> userFieldsByPath = extractFields(userFields, isKey);
        ClassDefinition classDefinition = findClassDefinition(isKey, options, serializationService);

        return userFields.isEmpty()
                ? resolveFields(isKey, classDefinition, serializationService)
                : resolveAndValidateFields(isKey, userFieldsByPath, classDefinition);
    }

    Stream<MappingField> resolveFields(
            boolean isKey,
            @Nullable ClassDefinition clazz,
            InternalSerializationService ss
    ) {
        if (clazz == null || clazz.getFieldCount() == 0) {
            // ClassDefinition does not exist, or it is empty, map the whole value
            String name = isKey ? KEY : VALUE;
            return Stream.of(new MappingField(name, QueryDataType.OBJECT, name));
        }

        return clazz.getFieldNames().stream()
                .map(name -> {
                    QueryPath path = new QueryPath(name, isKey);
                    QueryDataType type = TypesUtils.resolvePortableFieldType(clazz.getFieldType(name));

                    return new MappingField(name, type, path.toString());
                });
    }

    private static Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            Map<QueryPath, MappingField> userFieldsByPath,
            @Nullable ClassDefinition clazz
    ) {
        if (clazz == null) {
            // ClassDefinition does not exist, make sure there are no OBJECT fields
            return userFieldsByPath.values().stream()
                    .peek(mappingField -> {
                        QueryDataType type = mappingField.type();
                        if (type.getTypeFamily().equals(QueryDataTypeFamily.OBJECT)) {
                            throw QueryException.error("Cannot derive Portable type for '" + type.getTypeFamily() + "'");
                        }
                    });
        }

        for (String name : clazz.getFieldNames()) {
            final QueryPath path = new QueryPath(name, isKey);
            final QueryDataType type = TypesUtils.resolvePortableFieldType(clazz.getFieldType(name));

            MappingField userField = userFieldsByPath.get(path);
            if (userField != null && !type.getTypeFamily().equals(userField.type().getTypeFamily())) {
                throw QueryException.error("Mismatch between declared and resolved type: " + userField.name());
            }
        }
        return userFieldsByPath.values().stream();
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Map<QueryPath, MappingField> fieldsByPath = extractFields(resolvedFields, isKey);
        ClassDefinition clazz = resolveClassDefinition(isKey, options, fieldsByPath.values(), serializationService);

        return resolveMetadata(isKey, resolvedFields, fieldsByPath, clazz);
    }

    private static KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<QueryPath, MappingField> fieldsByPath,
            @Nonnull ClassDefinition clazz
    ) {
        List<TableField> fields = new ArrayList<>();
        for (Entry<QueryPath, MappingField> entry : fieldsByPath.entrySet()) {
            QueryPath path = entry.getKey();
            QueryDataType type = entry.getValue().type();
            String name = entry.getValue().name();

            fields.add(new MapTableField(name, type, false, path));
        }
        maybeAddDefaultField(isKey, resolvedFields, fields, QueryDataType.OBJECT);

        return new KvMetadata(
                fields,
                GenericQueryTargetDescriptor.DEFAULT,
                new PortableUpsertTargetDescriptor(clazz)
        );
    }

    @Nullable
    private static ClassDefinition findClassDefinition(
            boolean isKey,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Tuple3<Integer, Integer, Integer> settings = settings(isKey, options);
        //noinspection ConstantConditions
        return serializationService
                .getPortableContext()
                .lookupClassDefinition(settings.f0(), settings.f1(), settings.f2());
    }

    @Nonnull
    private static ClassDefinition resolveClassDefinition(
            boolean isKey,
            Map<String, String> options,
            Collection<MappingField> fields,
            InternalSerializationService serializationService
    ) {
        Tuple3<Integer, Integer, Integer> settings = settings(isKey, options);
        //noinspection ConstantConditions
        ClassDefinition classDefinition = serializationService
                .getPortableContext()
                .lookupClassDefinition(settings.f0(), settings.f1(), settings.f2());
        if (classDefinition != null) {
            return classDefinition;
        }

        ClassDefinitionBuilder classDefinitionBuilder = new ClassDefinitionBuilder(settings.f0(), settings.f1(), settings.f2());
        for (MappingField field : fields) {
            String name = field.name();
            QueryDataType type = field.type();
            switch (type.getTypeFamily()) {
                case BOOLEAN:
                    classDefinitionBuilder.addBooleanField(name);
                    break;
                case TINYINT:
                    classDefinitionBuilder.addByteField(name);
                    break;
                case SMALLINT:
                    classDefinitionBuilder.addShortField(name);
                    break;
                case INTEGER:
                    classDefinitionBuilder.addIntField(name);
                    break;
                case BIGINT:
                    classDefinitionBuilder.addLongField(name);
                    break;
                case REAL:
                    classDefinitionBuilder.addFloatField(name);
                    break;
                case DOUBLE:
                    classDefinitionBuilder.addDoubleField(name);
                    break;
                case DECIMAL:
                    classDefinitionBuilder.addDecimalField(name);
                    break;
                case VARCHAR:
                    classDefinitionBuilder.addStringField(name);
                    break;
                case TIME:
                    classDefinitionBuilder.addTimeField(name);
                    break;
                case DATE:
                    classDefinitionBuilder.addDateField(name);
                    break;
                case TIMESTAMP:
                    classDefinitionBuilder.addTimestampField(name);
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                    classDefinitionBuilder.addTimestampWithTimezoneField(name);
                    break;
                default:
                    // validated earlier, skip whole __key & this
            }
        }
        return classDefinitionBuilder.build();
    }

    private static Tuple3<Integer, Integer, Integer> settings(boolean isKey, Map<String, String> options) {
        String factoryIdProperty = isKey ? OPTION_KEY_FACTORY_ID : OPTION_VALUE_FACTORY_ID;
        String classIdProperty = isKey ? OPTION_KEY_CLASS_ID : OPTION_VALUE_CLASS_ID;
        String classVersionProperty = isKey ? OPTION_KEY_CLASS_VERSION : OPTION_VALUE_CLASS_VERSION;
        return Tuple3.tuple3(
                asInt(options, factoryIdProperty, null),
                asInt(options, classIdProperty, null),
                asInt(options, classVersionProperty, 0)
        );
    }
}
