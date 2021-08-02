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
import com.hazelcast.jet.sql.impl.extract.HazelcastJsonQueryTargetDescriptor;
import com.hazelcast.jet.sql.impl.inject.HazelcastJsonUpsertTargetDescriptor;
import com.hazelcast.sql.impl.schema.MappingField;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableField;
import com.hazelcast.sql.impl.type.QueryDataType;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.jet.sql.impl.connector.SqlConnector.JSON_TYPE_FORMAT;

final class MetadataJsonTypeResolver implements KvMetadataResolver {
    static final MetadataJsonTypeResolver INSTANCE = new MetadataJsonTypeResolver();

    private MetadataJsonTypeResolver() {
    }

    @Override
    public Stream<String> supportedFormats() {
        return Stream.of(JSON_TYPE_FORMAT);
    }

    @Override
    public Stream<MappingField> resolveAndValidateFields(
            boolean isKey,
            List<MappingField> userFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        if (isKey) {
            return Stream.of(new MappingField("__key", QueryDataType.JSON, "__key"));
        } else {
            return Stream.of(new MappingField("this", QueryDataType.JSON, "this"));
        }
    }

    @Override
    public KvMetadata resolveMetadata(
            boolean isKey,
            List<MappingField> resolvedFields,
            Map<String, String> options,
            InternalSerializationService serializationService
    ) {
        List<TableField> fields = Collections.singletonList(
                new MapTableField("this", QueryDataType.JSON, false, QueryPath.VALUE_PATH)
        );
        return new KvMetadata(
                fields,
                HazelcastJsonQueryTargetDescriptor.INSTANCE,
                HazelcastJsonUpsertTargetDescriptor.INSTANCE
        );
    }
}
