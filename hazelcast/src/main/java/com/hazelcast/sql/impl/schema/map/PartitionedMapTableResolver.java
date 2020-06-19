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
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.extract.GenericQueryTargetDescriptor;
import com.hazelcast.sql.impl.extract.QueryPath;
import com.hazelcast.sql.impl.extract.QueryTargetDescriptor;
import com.hazelcast.sql.impl.schema.ConstantTableStatistics;
import com.hazelcast.sql.impl.schema.ExternalCatalog;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableField;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadata;
import com.hazelcast.sql.impl.schema.map.sample.MapSampleMetadataResolver;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;
import static java.util.Collections.emptyList;

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

            Table table = createTable(nodeEngine, SCHEMA_NAME_PARTITIONED, mapName, Collections.emptyMap(), null, false);
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

    /**
     * @param explicitRequest True, if the table was requested explicitly
     *     through DDL. In this case the result is non-null and the returned
     *     table never contains an exception. If false, might return null or a
     *     table with an exception.
     */
    @Nullable
    public static PartitionedMapTable createTable(
            @Nonnull NodeEngine nodeEngine,
            @Nonnull String schemaName,
            @Nonnull String mapName,
            @Nonnull Map<String, String> options,
            @Nullable List<TableField> fields,
            boolean explicitRequest
    ) {
        try {
            MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
            MapServiceContext context = mapService.getMapServiceContext();
            MapContainer mapContainer = context.getMapContainer(mapName);

            // Handle concurrent map destroy.
            if (mapContainer == null) {
                if (explicitRequest) {
                    throw QueryException.error("Failed to resolve fields, map doesn't exist: " + mapName);
                }
                return null;
            }

            MapConfig config = mapContainer.getMapConfig();

            // HD maps are not supported at the moment.
            if (config.getInMemoryFormat() == InMemoryFormat.NATIVE) {
                throw QueryException.error("IMap with InMemoryFormat.NATIVE is not supported: " + mapName);
            }

            QueryTargetDescriptor keyDescriptor = null;
            QueryTargetDescriptor valueDescriptor = null;
            if (fields == null) {
                boolean binary = config.getInMemoryFormat() == InMemoryFormat.BINARY;

                for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
                    // Resolve sample.
                    RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(mapName);

                    if (recordStore == null) {
                        continue;
                    }

                    @SuppressWarnings("rawtypes")
                    Iterator<Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

                    if (!recordStoreIterator.hasNext()) {
                        continue;
                    }

                    @SuppressWarnings("rawtypes")
                    Map.Entry<Data, Record> entry = recordStoreIterator.next();

                    InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

                    MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(ss, entry.getKey(), binary, true);
                    MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(ss, entry.getValue().getValue(), binary, false);

                    keyDescriptor = keyMetadata.getDescriptor();
                    valueDescriptor = valueMetadata.getDescriptor();

                    fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());
                }

                if (fields == null) {
                    if (explicitRequest) {
                        throw QueryException.error("Failed to resolve fields, map is empty: " + mapName);
                    }
                    return emptyMap(mapName);
                }
            }

            if (keyDescriptor == null) {
                keyDescriptor = new GenericQueryTargetDescriptor();
            }
            if (valueDescriptor == null) {
                valueDescriptor = new GenericQueryTargetDescriptor();
            }

            long estimatedRowCount = MapTableUtils.estimatePartitionedMapRowCount(nodeEngine, context, mapName);

            // Map fields to ordinals.
            Map<QueryPath, Integer> pathToOrdinalMap = MapTableUtils.mapPathsToOrdinals(fields);

            List<MapTableIndex> indexes;
            int distributionFieldOrdinal;

            if (mapContainer != null) {
                // Resolve indexes.
                indexes = MapTableUtils.getPartitionedMapIndexes(mapContainer, mapName, pathToOrdinalMap);

                // Resolve distribution field ordinal.
                distributionFieldOrdinal = MapTableUtils.getPartitionedMapDistributionField(mapContainer, context, pathToOrdinalMap);
            } else {
                indexes = emptyList();
                distributionFieldOrdinal = PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE;
            }

            // Done.
            return new PartitionedMapTable(
                    schemaName,
                    mapName,
                    fields,
                    new ConstantTableStatistics(estimatedRowCount),
                    keyDescriptor,
                    valueDescriptor,
                    indexes,
                    distributionFieldOrdinal,
                    options);
        } catch (QueryException e) {
            return new PartitionedMapTable(mapName, e);
        } catch (Exception e) {
            QueryException e0 = QueryException.error("Failed to get metadata for IMap " + mapName + ": " + e.getMessage(), e);

            return new PartitionedMapTable(mapName, e0);
        }
    }

    private static PartitionedMapTable emptyMap(String mapName) {
        QueryException error = QueryException.error(
                "Cannot resolve IMap schema because it doesn't have entries on the local member: " + mapName
        );

        return new PartitionedMapTable(mapName, error);
    }
}
