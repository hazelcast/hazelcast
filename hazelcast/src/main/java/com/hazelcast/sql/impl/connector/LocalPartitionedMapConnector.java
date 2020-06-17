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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadata;
import com.hazelcast.sql.impl.schema.map.options.MapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.ObjectMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PojoMapOptionsMetadataResolver;
import com.hazelcast.sql.impl.schema.map.options.PortableMapOptionsMetadataResolver;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.sql.impl.schema.map.MapTableUtils.estimatePartitionedMapRowCount;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapDistributionField;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.getPartitionedMapIndexes;
import static com.hazelcast.sql.impl.schema.map.MapTableUtils.mapPathsToOrdinals;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

// TODO: do we want to keep it? maps are auto discovered...
public class LocalPartitionedMapConnector extends SqlKeyValueConnector {

    public static final String TYPE_NAME = "com.hazelcast.LocalPartitionedMap";

    private static final List<MapOptionsMetadataResolver> METADATA_RESOLVERS = asList(
            new ObjectMapOptionsMetadataResolver(),
            new PojoMapOptionsMetadataResolver(),
            new PortableMapOptionsMetadataResolver()
    );

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

    private static PartitionedMapTable createTable0(
            NodeEngine nodeEngine,
            String schemaName,
            String mapName,
            List<ExternalField> externalFields,
            Map<String, String> options
    ) {
        InternalSerializationService serializationService =
                (InternalSerializationService) nodeEngine.getSerializationService();

        MapOptionsMetadata keyMetadata = resolveMetadata(externalFields, options, true, serializationService);
        MapOptionsMetadata valueMetadata = resolveMetadata(externalFields, options, false, serializationService);
        List<TableField> fields = mergeFields(externalFields, keyMetadata.getFields(), valueMetadata.getFields());

        // TODO: deduplicate with PartitionedMapTableResolver ???
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = service.getMapServiceContext();
        MapContainer container = context.getMapContainer(mapName);

        long estimatedRowCount = estimatePartitionedMapRowCount(nodeEngine, context, mapName);
        Map<QueryPath, Integer> pathToOrdinalMap = mapPathsToOrdinals(fields);
        List<MapTableIndex> indexes =
                container != null ? getPartitionedMapIndexes(container, mapName, pathToOrdinalMap) : emptyList();
        int distributionFieldOrdinal =
                container != null ? getPartitionedMapDistributionField(container, context, pathToOrdinalMap) : -1;

        return new PartitionedMapTable(
                schemaName,
                mapName,
                fields,
                new ConstantTableStatistics(estimatedRowCount),
                keyMetadata.getQueryTargetDescriptor(),
                valueMetadata.getQueryTargetDescriptor(),
                keyMetadata.getUpsertTargetDescriptor(),
                valueMetadata.getUpsertTargetDescriptor(),
                indexes,
                distributionFieldOrdinal
        );
    }

    private static MapOptionsMetadata resolveMetadata(
            List<ExternalField> externalFields,
            Map<String, String> options,
            boolean key,
            InternalSerializationService serializationService
    ) {
        return METADATA_RESOLVERS
                .stream()
                .map(resolver -> resolver.resolve(externalFields, options, key, serializationService))
                .filter(Objects::nonNull)
                .findFirst()
                .orElseThrow(() ->
                        QueryException.error("Unable to resolve table metadata. Consult reference manual for more info.")
                );

        // TODO: fallback to sample resolution ???
    }
}
