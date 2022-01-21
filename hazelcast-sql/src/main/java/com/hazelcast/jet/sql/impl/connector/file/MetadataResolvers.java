/*
 * Copyright 2021 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.file;

import com.hazelcast.jet.sql.impl.connector.SqlConnector;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.QueryException;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.OPTION_FORMAT;
import static com.hazelcast.jet.sql.impl.connector.file.FileSqlConnector.OPTION_PATH;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

class MetadataResolvers {

    private final Map<Object, MetadataResolver<?>> resolvers;

    MetadataResolvers(MetadataResolver<?>... resolvers) {
        this.resolvers = Arrays.stream(resolvers).collect(toMap(MetadataResolver::supportedFormat, identity()));
    }

    /**
     * A utility to implement {@link SqlConnector#resolveAndValidateFields} in
     * the connector.
     */
    List<MappingField> resolveAndValidateFields(List<MappingField> userFields, Map<String, ?> options) {
        if (options.get(OPTION_FORMAT) == null) {
            throw QueryException.error("Missing '" + OPTION_FORMAT + "' option");
        }
        if (options.get(OPTION_PATH) == null) {
            throw QueryException.error("Missing '" + OPTION_PATH + "' option");
        }

        List<MappingField> fields = findMetadataResolver(options).resolveAndValidateFields(userFields, options);
        if (fields.isEmpty()) {
            throw QueryException.error("The resolved field list is empty");
        }
        return fields;
    }

    /**
     * A utility to implement {@link SqlConnector#createTable} in the
     * connector.
     */
    Metadata resolveMetadata(List<MappingField> resolvedFields, Map<String, ?> options) {
        return findMetadataResolver(options).resolveMetadata(resolvedFields, options);
    }

    private MetadataResolver<?> findMetadataResolver(Map<String, ?> options) {
        Object format = options.get(OPTION_FORMAT);
        MetadataResolver<?> resolver = resolvers.get(format);
        if (resolver == null) {
            throw QueryException.error("Unsupported serialization format: " + format);
        }
        return resolver;
    }
}
