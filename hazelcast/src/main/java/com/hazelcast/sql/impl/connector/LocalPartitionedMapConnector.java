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

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalTable.ExternalField;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.MapTableIndex;
import com.hazelcast.sql.impl.schema.map.MapTableUtils;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTable;
import com.hazelcast.sql.impl.schema.map.ResolverUtils;
import com.hazelcast.sql.impl.schema.map.ResolverUtils.ResolveResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class LocalPartitionedMapConnector extends LocalAbstractMapConnector {

    // TODO rename to LocalIMap
    public static final String TYPE_NAME = "com.hazelcast.LocalPartitionedMap";

    @Override
    public String typeName() {
        return TYPE_NAME;
    }

    @Nonnull @Override
    public Table createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String tableName,
            @Nonnull Map<String, String> options,
            @Nullable List<ExternalField> externalFields
    ) {
        String mapName = options.getOrDefault(TO_OBJECT_NAME, tableName);

        try {
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
            MapServiceContext context = mapService.getMapServiceContext();
            MapContainer mapContainer = context.getMapContainer(mapName);
            MapConfig config = mapContainer.getMapConfig();

            // HD maps are not supported at the moment.
            if (config.getInMemoryFormat() == InMemoryFormat.NATIVE) {
                throw QueryException.error("IMap with InMemoryFormat.NATIVE is not supported: " + mapName);
            }

            InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

            ResolveResult resolveResult;
            if (externalFields == null) {
                resolveResult = ResolverUtils.resolvePartitionedMap(ss, context, mapName);
                if (resolveResult == null) {
                    throw QueryException.error("Failed to get metadata for IMap " + mapName + ": map is empty");
                }
            } else {
                resolveResult = new ResolveResult(toMapFields(externalFields), new GenericQueryTargetDescriptor(),
                        new GenericQueryTargetDescriptor());
            }

            long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, mapName);

            // Map fields to ordinals.
            Map<QueryPath, Integer> pathToOrdinalMap = MapTableUtils.mapPathsToOrdinals(resolveResult.getFields());

            // Resolve indexes.
            List<MapTableIndex> indexes = MapTableUtils.getPartitionedMapIndexes(mapContainer, mapName, pathToOrdinalMap);

            // Resolve distribution field ordinal.
            int distributionFieldOrdinal =
                    MapTableUtils.getPartitionedMapDistributionField(mapContainer, context, pathToOrdinalMap);

            // Done.
            return new PartitionedMapTable(
                    schemaName,
                    mapName,
                    resolveResult.getFields(),
                    new ConstantTableStatistics(estimatedRowCount),
                    resolveResult.getKeyDescriptor(),
                    resolveResult.getValueDescriptor(),
                    indexes,
                    distributionFieldOrdinal,
                    options
            );
        } catch (QueryException e) {
            return new PartitionedMapTable(mapName, e);
        } catch (Exception e) {
            QueryException e0 = QueryException.error("Failed to get metadata for IMap " + mapName + ": " + e.getMessage(), e);

            return new PartitionedMapTable(mapName, e0);
        }

    }
}
