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

package com.hazelcast.jet.sql.impl.connector.map;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadata;
import com.hazelcast.jet.sql.impl.connector.keyvalue.KvMetadataResolver;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.MappingField;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.COMPACT_FORMAT;

final class MetadataCompactDisabledResolver implements KvMetadataResolver {

    static final MetadataCompactDisabledResolver INSTANCE = new MetadataCompactDisabledResolver();

    private MetadataCompactDisabledResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(COMPACT_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        throw QueryException.error("Compact serialization is disabled in the config");
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        throw QueryException.error("Compact serialization is disabled in the config");
    }
}
