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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import javax.annotation.CheckReturnValue;
import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_CLASS;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PREFIX;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;

/**
 * A utility to resolve fields for key-value connectors that support
 * multiple serialization methods.
 */
public class KvMetadataResolvers {

    // A string of characters (excluding a `.`), optionally prefixed with "__key." or "this."
    private static final Pattern EXT_NAME_PATTERN = Pattern.compile("((" + KEY + "|" + VALUE + ")\\.)?[^.]+");

    private final Map<String, KvMetadataResolver> keyResolvers;
    private final Map<String, KvMetadataResolver> valueResolvers;

    public KvMetadataResolvers(KvMetadataResolver... resolvers) {
        this(resolvers, resolvers);
    }

    public KvMetadataResolvers(
            KvMetadataResolver[] keyResolvers,
            KvMetadataResolver[] valueResolvers
    ) {
        this.keyResolvers = resolversMap(keyResolvers);
        this.valueResolvers = resolversMap(valueResolvers);
    }

    private Map<String, KvMetadataResolver> resolversMap(KvMetadataResolver[] resolvers) {
        return Arrays.stream(resolvers)
                .collect(toMap(KvMetadataResolver::supportedFormat, Function.identity()));
    }

    /**
     * A utility to implement {@link SqlConnector#resolveAndValidateFields} in
     * the connector.
     */
    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            NodeEngine nodeEngine
    ) {
        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        // normalize and validate the names and external names
        for (MappingField field : userFields) {
            String name = field.name();

            // resolve the external name, if not specified
            String extName = field.externalName();
            if (extName == null) {
                if (name.equals(KEY) || name.equals(VALUE)) {
                    extName = name;
                } else {
                    extName = VALUE_PREFIX + name;
                }
                field.setExternalName(name);
            }

            if (!EXT_NAME_PATTERN.matcher(extName).matches()) {
                throw QueryException.error("Invalid external name: " + extName);
            }
        }

        options = preprocessOptions(options, true);
        options = preprocessOptions(options, false);

        List<MappingField> keyFields = findMetadataResolver(options, true)
                .resolveAndValidateFields(true, userFields, options, ss);
        List<MappingField> valueFields = findMetadataResolver(options, false)
                .resolveAndValidateFields(false, userFields, options, ss);

        Map<String, MappingField> fields = concat(keyFields.stream(), valueFields.stream())
                .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        if (fields.isEmpty()) {
            throw QueryException.error("The resolved field list is empty");
        }

        return new ArrayList<>(fields.values());
    }

    /**
     * A utility to implement {@link SqlConnector#createTable} in the
     * connector.
     */
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        options = preprocessOptions(options, isKey);
        KvMetadataResolver resolver = findMetadataResolver(options, isKey);
        return requireNonNull(resolver.resolveMetadata(isKey, resolvedFields, options, serializationService));
    }

    @CheckReturnValue
    private Map<String, String> preprocessOptions(Map<String, String> options, boolean isKey) {
        String formatProperty = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;

        String formatValue = options.get(formatProperty);
        if (formatValue == null) {
            return options;
        }
        String classValue;
        switch (formatValue) {
            case "varchar":
            case "character varying":
            case "char varying":
                classValue = String.class.getName();
                break;

            case "boolean":
                classValue = Boolean.class.getName();
                break;

            case "tinyint":
                classValue = Byte.class.getName();
                break;

            case "smallint":
                classValue = Short.class.getName();
                break;

            case "integer":
            case "int":
                classValue = Integer.class.getName();
                break;

            case "bigint":
                classValue = Long.class.getName();
                break;

            case "decimal":
            case "dec":
            case "numeric":
                classValue = BigDecimal.class.getName();
                break;

            case "real":
                classValue = Float.class.getName();
                break;

            case "double":
            case "double precision":
                classValue = Double.class.getName();
                break;

            case "time":
                classValue = LocalTime.class.getName();
                break;

            case "date":
                classValue = LocalDate.class.getName();
                break;

            case "timestamp":
                classValue = LocalDateTime.class.getName();
                break;

            case "timestamp with time zone":
                classValue = OffsetDateTime.class.getName();
                break;

            default:
                classValue = null;
        }

        String classProperty = isKey ? OPTION_KEY_CLASS : OPTION_VALUE_CLASS;
        if (classValue != null && !options.containsKey(classProperty)) {
            Map<String, String> newOptions = new HashMap<>(options);
            newOptions.put(formatProperty, "java");
            newOptions.put(classProperty, classValue);
            return newOptions;
        }

        return options;
    }

    private KvMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;
        String format = options.get(option);
        KvMetadataResolver resolver = (isKey ? keyResolvers : valueResolvers).get(format);
        if (resolver == null) {
            if (format == null) {
                throw QueryException.error("Missing '" + option + "' option");
            }
            throw QueryException.error("Unsupported serialization format: " + format);
        }
        return resolver;
    }

    public static Map<QueryPath, MappingField> extractFields(List<MappingField> fields, boolean isKey) {
        Map<QueryPath, MappingField> fieldsByPath = new LinkedHashMap<>();
        for (MappingField field : fields) {
            QueryPath path = QueryPath.create(field.externalName());
            if (isKey != path.isKey()) {
                continue;
            }
            if (fieldsByPath.putIfAbsent(path, field) != null) {
                throw QueryException.error("Duplicate external name: " + path);
            }
        }
        return fieldsByPath;
    }

    public static void maybeAddDefaultField(
            boolean isKey,
            @Nonnull List<MappingField> resolvedFields,
            @Nonnull List<TableField> tableFields
    ) {
        // Add the default `__key` or `this` field as hidden, if present neither in the external
        // names, nor in the field names
        String fieldName = isKey ? KEY : VALUE;
        if (resolvedFields.stream().noneMatch(f -> f.externalName().equals(fieldName) || f.name().equals(fieldName))) {
            tableFields.add(new MapTableField(fieldName, QueryDataType.OBJECT, true, QueryPath.create(fieldName)));
        }
    }
}
