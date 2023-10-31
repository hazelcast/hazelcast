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

package com.hazelcast.jet.sql.impl.connector.keyvalue;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.jet.sql.impl.schema.RelationsStorage;
import com.hazelcast.jet.sql.impl.schema.TypeUtils.FieldEnricher;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.SqlServiceImpl;
import com.hazelcast.sql.impl.schema.MappingField;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver.extractFields;
import static com.hazelcast.jet.sql.impl.schema.TypeUtils.getFieldEnricher;
import static com.hazelcast.sql.impl.extract.QueryPath.KEY;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE;
import static com.hazelcast.sql.impl.extract.QueryPath.VALUE_PREFIX;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

/**
 * A utility to resolve fields for key-value connectors that support
 * multiple serialization methods.
 */
public class KvMetadataResolvers {

    // A string of characters (excluding a `.`), optionally prefixed with "__key." or "this."
    private static final Pattern EXT_NAME_PATTERN = Pattern.compile("((" + KEY + "|" + VALUE + ")\\.)?[^.]+");
    private static final Set<String> NESTED_FIELDS_SUPPORTED_FORMATS = Set.of(
            SqlConnector.JAVA_FORMAT, SqlConnector.PORTABLE_FORMAT,
            SqlConnector.COMPACT_FORMAT, SqlConnector.AVRO_FORMAT);

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
                .flatMap(resolver -> resolver.supportedFormats().map(format -> entry(format, resolver)))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * A utility to implement {@link SqlConnector#resolveAndValidateFields} in the connector.
     */
    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            NodeEngine nodeEngine
    ) {
        final InternalSerializationService serializationService = (InternalSerializationService) nodeEngine
                .getSerializationService();
        final RelationsStorage relationsStorage = ((SqlServiceImpl) nodeEngine.getSqlService()).getOptimizer()
                .relationsStorage();
        // normalize and validate the names and external names
        for (MappingField field : userFields) {
            String name = field.name();

            String externalName = field.externalName();
            if (externalName == null) {
                if (name.equals(KEY) || name.equals(VALUE)) {
                    externalName = name;
                } else {
                    externalName = VALUE_PREFIX + name;
                }
                field.setExternalName(name);
            }

            if ((name.equals(KEY) && !externalName.equals(KEY))
                    || (name.equals(VALUE) && !externalName.equals(VALUE))) {
                throw QueryException.error("Cannot rename field: '" + name + '\'');
            }

            if (!EXT_NAME_PATTERN.matcher(externalName).matches()) {
                throw QueryException.error("Invalid external name: " + externalName);
            }
        }

        Stream<MappingField> keyFields = resolveAndValidateFields(true, userFields, options,
                serializationService, relationsStorage);
        Stream<MappingField> valueFields = resolveAndValidateFields(false, userFields, options,
                serializationService, relationsStorage);

        Map<String, MappingField> fields = Stream.concat(keyFields, valueFields)
                .collect(LinkedHashMap::new, (map, field) -> map.putIfAbsent(field.name(), field), Map::putAll);

        if (fields.isEmpty()) {
            throw QueryException.error("The resolved field list is empty");
        }
        return new ArrayList<>(fields.values());
    }

    private Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService,
            RelationsStorage relationsStorage
    ) {
        String format = getFormat(options, isKey);
        if (format != null && NESTED_FIELDS_SUPPORTED_FORMATS.contains(format)) {
            List<MappingField> fieldsWithCustomTypes = extractFields(userFields, isKey).values().stream()
                    .filter(mappingField -> mappingField.type().isCustomType()).collect(toList());
            if (!fieldsWithCustomTypes.isEmpty()) {
                FieldEnricher<?, ?> enricher = getFieldEnricher(format, serializationService, relationsStorage);
                fieldsWithCustomTypes.forEach(mappingField -> enricher.enrich(mappingField, options, isKey));
            }
        }

        String name = isKey ? KEY : VALUE;
        return findMetadataResolver(options, isKey)
                .resolveAndValidateFields(isKey, userFields, options, serializationService)
                .filter(field -> !field.name().equals(name) || field.externalName().equals(name));
    }

    /**
     * A utility to implement {@link SqlConnector#createTable} in the connector.
     */
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        KvMetadataResolver resolver = findMetadataResolver(options, isKey);
        return requireNonNull(resolver.resolveMetadata(isKey, resolvedFields, options, serializationService));
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

    /**
     * Null format is only valid for Kafka keys.
     * @see KvMetadataNullResolver
     */
    @Nullable
    private static String getFormat(Map<String, String> options, boolean isKey) {
        return options.get(isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT);
    }
}
