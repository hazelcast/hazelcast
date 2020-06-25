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

package com.hazelcast.sql.impl.connector;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;
import com.hazelcast.sql.impl.schema.map.options.JsonMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadata;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PojoMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PortableMapOptionsMetadataResolver;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.stream.Collectors.toMap;

// TODO: do we want to keep it? maps are auto discovered...
public class LocalReplicatedMapConnector extends SqlKeyValueConnector {

    public static final String TYPE_NAME = "com.hazelcast.LocalReplicatedMap";

    private static final Map<String, MapOptionsMetadataResolver> METADATA_RESOLVERS = Stream.of(
            new PojoMapOptionsMetadataResolver(),
            new PortableMapOptionsMetadataResolver(),
            new JsonMapOptionsMetadataResolver()
    ).collect(toMap(MapOptionsMetadataResolver::supportedFormat, Function.identity()));

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String name,
            @Nonnull List<ExternalField> externalFields,
            @Nonnull Map<String, String> options
    ) {
        String objectName = options.getOrDefault(TO_OBJECT_NAME, name);
        return createTable0(nodeEngine, schemaName, objectName, externalFields, options);
    }

    private static ReplicatedMapTable createTable0(
            NodeEngine nodeEngine,
            String schemaName,
            String name,
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        InternalSerializationService serializationService =
                (InternalSerializationService) nodeEngine.getSerializationService();

        MapOptionsMetadata keyMetadata = resolveMetadata(externalFields, options, true, serializationService);
        MapOptionsMetadata valueMetadata = resolveMetadata(externalFields, options, false, serializationService);
        List<TableField> fields = mergeFields(externalFields, keyMetadata.getFields(), valueMetadata.getFields());

        // TODO: deduplicate with ReplicatedMapTableResolver ???
        ReplicatedMapService service = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(name);

        long estimatedRowCount = stores.size() * nodeEngine.getPartitionService().getPartitionCount();

        return new ReplicatedMapTable(
                schemaName,
                name,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor()
        );
    }

    private static MapOptionsMetadata resolveMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean key,
            InternalSerializationService serializationService
    ) {
        String format = options.get(key ? TO_SERIALIZATION_KEY_FORMAT : TO_SERIALIZATION_VALUE_FORMAT);
        if (format == null) {
            return MapOptionsMetadataResolver.resolve(externalFields, key);
        }

        MapOptionsMetadataResolver resolver = METADATA_RESOLVERS.get(format);
        if (resolver == null) {
            throw QueryException.error(
                    format("Specified format '%s' is not among supported ones %s", format, METADATA_RESOLVERS.keySet())
            );
        }

        return checkNotNull(resolver.resolve(externalFields, options, key, serializationService));
    }
}
