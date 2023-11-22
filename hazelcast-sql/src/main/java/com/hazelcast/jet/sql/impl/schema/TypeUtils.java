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

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.portable.PortableContext;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.FieldDefinition;
import com.hazelcast.nio.serialization.FieldType;
import com.hazelcast.nio.serialization.PortableId;
import com.hazelcast.sql.impl.FieldsUtil;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.schema.type.Type;
import com.hazelcast.sql.impl.schema.type.Type.TypeField;
import com.hazelcast.sql.impl.schema.type.TypeKind;
import com.hazelcast.sql.impl.type.QueryDataType;
import com.hazelcast.sql.impl.type.QueryDataType.QueryDataTypeField;
import com.hazelcast.sql.impl.type.QueryDataTypeUtils;
import org.apache.avro.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.function.Supplier;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.AVRO_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JAVA_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_AVRO_SCHEMA;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_COMPACT_TYPE_NAME;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_JAVA_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_CLASS_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_CLASS_VERSION;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_TYPE_PORTABLE_FACTORY_ID;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.PORTABLE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.AvroResolver.unwrapNullableType;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataAvroResolver.inlineSchema;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataJavaResolver.loadClass;
import static com.hazelcast.jet.sql.impl.connector.map.MetadataCompactResolver.compactTypeName;
import static com.hazelcast.jet.sql.impl.connector.map.MetadataPortableResolver.PORTABLE_TO_SQL;
import static com.hazelcast.jet.sql.impl.connector.map.MetadataPortableResolver.portableId;
import static java.util.stream.Collectors.toList;

public final class TypeUtils {
    private TypeUtils() { }

    public static FieldEnricher<?, ?> getFieldEnricher(
            String format,
            InternalSerializationService serializationService,
            RelationsStorage relationsStorage
    ) {
        switch (format) {
            case PORTABLE_FORMAT:
                return new PortableEnricher(relationsStorage, serializationService);
            case COMPACT_FORMAT:
                return new CompactEnricher(relationsStorage);
            case JAVA_FORMAT:
                return new JavaEnricher(relationsStorage);
            case AVRO_FORMAT:
                return new AvroEnricher(relationsStorage);
            default:
                throw QueryException.error("Unsupported type format: " + format);
        }
    }

    private static class PortableEnricher extends FieldEnricher<PortableId, ClassDefinition> {
        private final PortableContext context;

        PortableEnricher(RelationsStorage relationsStorage, InternalSerializationService serializationService) {
            super(TypeKind.PORTABLE, relationsStorage);
            context = serializationService.getPortableContext();
        }

        @Override
        protected String getTypeMetadata(PortableId portableId) {
            return portableId.toString();
        }

        @Override
        protected ClassDefinition getSchema(PortableId portableId) {
            return context.lookupClassDefinition(portableId);
        }

        @Override
        protected List<TypeField> resolveFields(ClassDefinition classDef) {
            if (classDef == null) {
                throw QueryException.error("Either a column list must be provided or the class "
                        + "definition must be registered to create Portable-based types");
            }
            return classDef.getFieldNames().stream().map(name -> {
                FieldDefinition field = classDef.getField(name);
                if (field.getType().equals(FieldType.PORTABLE)) {
                    throw QueryException.error("Column list is required to create nested fields");
                }
                return new TypeField(name, PORTABLE_TO_SQL.getOrDefault(field.getType()));
            }).collect(toList());
        }

        @Override
        protected PortableId getFieldSchemaId(ClassDefinition classDef, String fieldName, String fieldTypeName) {
            if (classDef == null) {
                throw QueryException.error("Either a portable ID must be provided or the "
                        + "class definition must be registered to create nested fields");
            }
            return classDef.getField(fieldName).getPortableId();
        }

        @Override
        protected PortableId getSchemaId(Map<String, String> mappingOptions, boolean isKey) {
            return portableId(mappingOptions, isKey);
        }

        @Override
        protected PortableId getSchemaId(Map<String, String> typeOptions) {
            return portableId(typeOptions, OPTION_TYPE_PORTABLE_FACTORY_ID,
                    OPTION_TYPE_PORTABLE_CLASS_ID, OPTION_TYPE_PORTABLE_CLASS_VERSION);
        }
    }

    private static class CompactEnricher extends FieldEnricher<String, Void> {
        CompactEnricher(RelationsStorage relationsStorage) {
            super(TypeKind.COMPACT, relationsStorage);
        }

        @Override
        protected String getTypeMetadata(String compactTypeName) {
            return compactTypeName;
        }

        @Override
        protected Void getSchema(String schemaId) {
            return null;
        }

        @Override
        protected List<TypeField> resolveFields(Void schema) {
            throw QueryException.error("Column list is required to create Compact-based types");
        }

        @Override
        protected String getFieldSchemaId(Void schema, String fieldName, String fieldTypeName) {
            return fieldTypeName + "CompactType";
        }

        @Override
        protected String getSchemaId(Map<String, String> mappingOptions, boolean isKey) {
            return compactTypeName(mappingOptions, isKey);
        }

        @Override
        protected String getSchemaId(Map<String, String> typeOptions) {
            return typeOptions.get(OPTION_TYPE_COMPACT_TYPE_NAME);
        }
    }

    private static class JavaEnricher extends FieldEnricher<Class<?>, SortedMap<String, Class<?>>> {
        JavaEnricher(RelationsStorage relationsStorage) {
            super(TypeKind.JAVA, relationsStorage);
        }

        @Override
        protected String getTypeMetadata(Class<?> typeClass) {
            return typeClass.getName();
        }

        @Override
        protected SortedMap<String, Class<?>> getSchema(Class<?> typeClass) {
            return FieldsUtil.resolveClass(typeClass);
        }

        @Override
        protected List<TypeField> resolveFields(SortedMap<String, Class<?>> classFields) {
            return classFields.entrySet().stream().map(e -> {
                if (isUserClass(e.getValue())) {
                    throw QueryException.error("Column list is required to create nested fields");
                }
                return new TypeField(e.getKey(), QueryDataTypeUtils.resolveTypeForClass(e.getValue()));
            }).collect(toList());
        }

        @Override
        protected Class<?> getFieldSchemaId(SortedMap<String, Class<?>> classFields,
                                            String fieldName, String fieldTypeName) {
            return classFields.get(fieldName);
        }

        @Override
        protected Class<?> getSchemaId(Map<String, String> mappingOptions, boolean isKey) {
            return loadClass(mappingOptions, isKey);
        }

        @Override
        protected Class<?> getSchemaId(Map<String, String> typeOptions) {
            String className = typeOptions.get(OPTION_TYPE_JAVA_CLASS);
            return className != null ? loadClass(className) : null;
        }

        private static boolean isUserClass(Class<?> clazz) {
            return !clazz.isPrimitive() && !clazz.getPackage().getName().startsWith("java.");
        }
    }

    private static class AvroEnricher extends FieldEnricher<Schema, Schema> {
        AvroEnricher(RelationsStorage relationsStorage) {
            super(TypeKind.AVRO, relationsStorage);
        }

        @Override
        protected String getTypeMetadata(Schema schema) {
            // AvroUpsertTarget has already a reference to the schema, and Avro schemas
            // are self-contained. That is, it is not possible to have a partial schema
            // or override some parts of a schema.
            return null;
        }

        @Override
        protected Schema getSchema(Schema schema) {
            return schema;
        }

        @Override
        protected List<TypeField> resolveFields(Schema schema) {
            if (schema == null) {
                throw QueryException.error(
                        "Either a column list or an inline schema is required to create Avro-based types");
            }
            return KvMetadataAvroResolver.resolveFields(schema, TypeField::new).collect(toList());
        }

        @Override
        protected Schema getFieldSchemaId(Schema schema, String fieldName, String fieldTypeName) {
            return schema != null ? unwrapNullableType(schema.getField(fieldName).schema()) : null;
        }

        @Override
        protected Schema getSchemaId(Map<String, String> mappingOptions, boolean isKey) {
            return inlineSchema(mappingOptions, isKey);
        }

        @Override
        protected Schema getSchemaId(Map<String, String> typeOptions) {
            String json = typeOptions.get(OPTION_TYPE_AVRO_SCHEMA);
            return json != null ? new Schema.Parser().parse(json) : null;
        }
    }

    /**
     * Adds {@linkplain QueryDataType#getObjectTypeMetadata() metadata}, i.e. schema ID,
     * to custom types. <ol>
     * <li> If the type has a well-defined† metadata in options, this self-reported metadata
     *      is used. († For example, in Portable, {@code factoryId}, {@code classId}, and
     *      {@code version} must be provided together.)
     * <li> If the mapping has only {@code __key} or {@code this} fields, and both the mapping
     *      and type define metadata, the one that is defined by the type is used. In other
     *      words, type options override mapping options. In this case, the mapping does not
     *      need to specify metadata since it will be ignored.
     * <li> If the type does not define metadata, it is resolved from the parent, which might
     *      be another type or the mapping: if the type belongs to explicit {@code __key} or
     *      {@code this} fields, the metadata will be <em>inherited</em> from the mapping;
     *      otherwise, it will be <em>accessed</em> as a field from the parent schema.
     * </ol>
     *
     * @param <ID> type of schema identifier
     * @param <S> type of schema
     */
    public abstract static class FieldEnricher<ID, S> {
        private final TypeKind typeKind;
        private final RelationsStorage relationsStorage;

        FieldEnricher(TypeKind typeKind, RelationsStorage relationsStorage) {
            this.typeKind = typeKind;
            this.relationsStorage = relationsStorage;
        }

        public void enrich(MappingField field, Map<String, String> mappingOptions, boolean isKey) {
            String typeName = field.type().getObjectTypeName();
            field.setType(createFieldType(
                    field.name().equals(isKey ? QueryPath.KEY : QueryPath.VALUE)
                            ? () -> getSchemaId(mappingOptions, isKey)
                            : () -> getFieldSchemaId(getSchema(getSchemaId(mappingOptions, isKey)),
                                    plainExternalName(field), typeName),
                    typeName, new HashMap<>()));
        }

        protected QueryDataType createFieldType(Supplier<ID> schemaIdSupplier, String typeName,
                                                Map<String, QueryDataType> seen) {
            QueryDataType convertedType = seen.get(typeName);
            if (convertedType != null) {
                return convertedType;
            }

            Type type = relationsStorage.getType(typeName);
            if (type == null) {
                throw QueryException.error("Encountered type '" + typeName + "', which doesn't exist");
            }

            ID schemaId = getSchemaId(type.options());
            if (schemaId == null) {
                schemaId = schemaIdSupplier.get();
            }

            S schema = getSchema(schemaId);
            if (type.getFields().isEmpty()) {
                type.setFields(resolveFields(schema));
                relationsStorage.put(typeName, type);
            }

            convertedType = new QueryDataType(typeName, typeKind, getTypeMetadata(schemaId));
            seen.put(typeName, convertedType);

            for (TypeField field : type.getFields()) {
                QueryDataType fieldType = field.getType();
                String fieldTypeName = fieldType.getObjectTypeName();

                if (fieldType.isCustomType()) {
                    fieldType = createFieldType(() -> getFieldSchemaId(schema, field.getName(), fieldTypeName),
                            fieldTypeName, seen);
                }
                convertedType.getObjectFields().add(new QueryDataTypeField(field.getName(), fieldType));
            }

            return convertedType;
        }

        protected abstract String getTypeMetadata(ID schemaId);
        protected abstract S getSchema(ID schemaId);
        protected abstract List<TypeField> resolveFields(S schema);
        protected abstract ID getFieldSchemaId(S schema, String fieldName, String fieldTypeName);
        protected abstract ID getSchemaId(Map<String, String> mappingOptions, boolean isKey);
        protected abstract ID getSchemaId(Map<String, String> typeOptions);

        /**
         * Returns the external name of the field without {@code __key.} or {@code this.} prefix.
         */
        public static String plainExternalName(MappingField field) {
            return QueryPath.create(field.externalName()).getPath();
        }
    }
}
