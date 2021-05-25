/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.sql.impl.connector.map.journal;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_KEY_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_VALUE_FORMAT;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public class IMapJournalMetadataResolvers {
    private final Map<String, IMapJournalMetadataResolver> resolversMap;

    public IMapJournalMetadataResolvers(IMapJournalMetadataResolver... resolvers) {
        resolversMap = new HashMap<>();
        for (IMapJournalMetadataResolver resolver : resolvers) {
            resolver.supportedFormats().forEach(f -> resolversMap.put(f, resolver));
        }
    }

    public List<MappingField> resolveAndValidateFields(
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        assert userFields.size() == 0;
        List<MappingField> keyMappings =
                findMetadataResolver(options, true).resolveAndValidateFields(
                        true,
                        userFields,
                        options,
                        serializationService
                );
        List<MappingField> typeMapping = singletonList(
                new MappingField("type", QueryDataType.VARCHAR, "type"));
        List<MappingField> valueMappings =
                findMetadataResolver(options, true).resolveAndValidateFields(
                        false,
                        userFields,
                        options,
                        serializationService
                );

        return Stream.concat(Stream.concat(keyMappings.stream(), typeMapping.stream()), valueMappings.stream()).collect(toList());
    }

    public Metadata resolveMetadata(
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        Metadata keyMetadata =  findMetadataResolver(options, true)
                .resolveMetadata(true, resolvedFields, options, serializationService);
        Metadata valueMetadata = findMetadataResolver(options, false)
                .resolveMetadata(false, resolvedFields, options, serializationService);

        List<TableField> typeMapping = singletonList(new TableField("type", QueryDataType.VARCHAR, false));

        List<TableField> fields = Stream.concat(
                Stream.concat(keyMetadata.getFields().stream(), typeMapping.stream()),
                valueMetadata.getFields().stream()).collect(toList());
        return new Metadata(
                fields,
                keyMetadata.getKeyQueryTargetDescriptor(),
                valueMetadata.getValueQueryTargetDescriptor()
        );
    }

    private IMapJournalMetadataResolver findMetadataResolver(Map<String, String> options, boolean isKey) {
        String option = isKey ? OPTION_KEY_FORMAT : OPTION_VALUE_FORMAT;
        String format = options.get(option);
        if (format == null) {
            throw QueryException.error("Missing option: " + option);
        }
        IMapJournalMetadataResolver resolver = resolversMap.get(format);
        if (resolver == null) {
            throw QueryException.error("Unsupported serialization format: " + format);
        }
        return resolver;
    }
}
