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

package com.hazelcast.sql.impl.schema.map;

import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.map.MapTableUtils.ResolveResult;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;

public class PartitionedMapTableResolver extends AbstractMapTableResolver {

    private static final List<List<String>> SEARCH_PATHS =
        Collections.singletonList(Arrays.asList(QueryUtils.CATALOG, SCHEMA_NAME_PARTITIONED));

    public PartitionedMapTableResolver(NodeEngine nodeEngine) {
        super(nodeEngine, SEARCH_PATHS);
    }

    @Override @Nonnull
    public Collection<Table> getTables() {
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        MapServiceContext context = mapService.getMapServiceContext();

        List<Table> res = new ArrayList<>();
        Set<String> knownNames = new HashSet<>();

        // Get started maps.
        for (String mapName : context.getMapContainers().keySet()) {
            // TODO: skip all system tables, i.e. `__jet` prefixed
            if (mapName.equalsIgnoreCase(ExternalCatalog.CATALOG_MAP_NAME)) {
                continue;
            }

            PartitionedMapTable table = createTable(context, mapName);

            if (table == null) {
                continue;
            }

            res.add(table);
            knownNames.add(mapName);
        }

        // Get maps that are not started locally yet.
        for (Map.Entry<String, MapConfig> configEntry : nodeEngine.getConfig().getMapConfigs().entrySet()) {
            String configMapName = configEntry.getKey();

            // Skip templates.
            // TODO take ConfigPatternMatcher into account
            if (configMapName.contains("*")) {
                continue;
            }

            if (knownNames.add(configMapName)) {
                res.add(emptyMap(configMapName));
            }
        }

        return res;
    }

    @SuppressWarnings({"checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    private PartitionedMapTable createTable(
        MapServiceContext context,
        String name
    ) {
        try {
            MapContainer mapContainer = context.getMapContainer(name);

            // Handle concurrent map destroy.
            if (mapContainer == null) {
                return null;
            }

            MapConfig config = mapContainer.getMapConfig();

            // HD maps are not supported at the moment.
            if (config.getInMemoryFormat() == InMemoryFormat.NATIVE) {
                throw QueryException.error("IMap with InMemoryFormat.NATIVE is not supported: " + name);
            }

            InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

            ResolveResult resolved = MapTableUtils.resolvePartitionedMap(ss, context, name);
            if (resolved == null) {
                return emptyMap(name);
            }

            long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, name);

            // Map fields to ordinals.
            Map<QueryPath, Integer> pathToOrdinalMap = MapTableUtils.mapPathsToOrdinals(resolved.getFields());

            // Resolve indexes.
            List<MapTableIndex> indexes = MapTableUtils.getPartitionedMapIndexes(mapContainer, name, pathToOrdinalMap);

            // Resolve distribution field ordinal.
            int distributionFieldOrdinal =
                MapTableUtils.getPartitionedMapDistributionField(mapContainer, context, pathToOrdinalMap);

            // Done.
            return new PartitionedMapTable(
                    SCHEMA_NAME_PARTITIONED,
                    name,
                    resolved.getFields(),
                    new ConstantTableStatistics(estimatedRowCount),
                    resolved.getKeyDescriptor(),
                    resolved.getValueDescriptor(),
                    null,
                    null,
                    indexes,
                    distributionFieldOrdinal
            );
        } catch (QueryException e) {
            return new PartitionedMapTable(name, e);
        } catch (Exception e) {
            QueryException e0 = QueryException.error("Failed to get metadata for IMap " + name + ": " + e.getMessage(), e);

            return new PartitionedMapTable(name, e0);
        }
    }

    private static PartitionedMapTable emptyMap(String mapName) {
        QueryException error = QueryException.error(
            "Cannot resolve IMap schema because it doesn't have entries on the local member: " + mapName
        );

        return new PartitionedMapTable(mapName, error);
    }
}
