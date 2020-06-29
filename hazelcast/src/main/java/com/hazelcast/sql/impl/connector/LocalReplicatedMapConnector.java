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
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.ReplicatedMapTable;
import com.hazelcast.sql.impl.schema.map.options.JsonMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadata;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.JavaMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PortableMapOptionsMetadataResolver;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

// TODO: do we want to keep it? maps are auto discovered...
public class LocalReplicatedMapConnector extends SqlKeyValueConnector {

    public static final String TYPE_NAME = "com.hazelcast.ReplicatedMap";

    private static final Map<String, MapOptionsMetadataResolver> METADATA_RESOLVERS = Stream.of(
            JavaMapOptionsMetadataResolver.INSTANCE,
            PortableMapOptionsMetadataResolver.INSTANCE,
            JsonMapOptionsMetadataResolver.INSTANCE
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
            @Nonnull Map<String, String> options,
            @Nonnull List<ExternalField> externalFields
    ) {
        String mapName = options.getOrDefault(TO_OBJECT_NAME, name);

        InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

        MapOptionsMetadata keyMetadata = resolveMetadata(externalFields, options, true, ss);
        MapOptionsMetadata valueMetadata = resolveMetadata(externalFields, options, false, ss);
        List<TableField> fields = mergeFields(keyMetadata.getFields(), valueMetadata.getFields());

        // TODO: deduplicate with ReplicatedMapTableResolver ???
        ReplicatedMapService service = nodeEngine.getService(ReplicatedMapService.SERVICE_NAME);
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(mapName);

        long estimatedRowCount = stores.size() * nodeEngine.getPartitionService().getPartitionCount();

        return new ReplicatedMapTable(
                schemaName,
                mapName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor()
        );
    }

    @Override
    protected Map<String, MapOptionsMetadataResolver> supportedResolvers() {
        return METADATA_RESOLVERS;
    }
}
