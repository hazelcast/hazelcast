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

import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.hazelcast.sql.impl.QueryUtils.SCHEMA_NAME_PARTITIONED;
import static java.util.Collections.emptyMap;

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

        for (String mapName : context.getMapContainers().keySet()) {
            PartitionedMapTable table;

            // TODO: skip all system tables, i.e. `__jet` prefixed
            if (mapName.equalsIgnoreCase(ExternalCatalog.CATALOG_MAP_NAME)) {
                continue;
            }

            try {
                table = createTable(SCHEMA_NAME_PARTITIONED, nodeEngine, context, mapName, null, emptyMap());
            } catch (QueryException e) {
                table = new PartitionedMapTable(mapName, e);
            }

            if (table == null) {
                continue;
            }

            res.add(table);
        }

        return res;
    }

    @SuppressWarnings({"rawtypes", "checkstyle:MethodLength", "checkstyle:CyclomaticComplexity", "checkstyle:NPathComplexity"})
    public static PartitionedMapTable createTable(
            @Nonnull String schemaName,
            @Nonnull NodeEngine nodeEngine,
            @Nonnull MapServiceContext context,
            @Nonnull String mapName,
            @Nullable List<TableField> fields,
            @Nonnull Map<String, String> ddlOptions
    ) {
        try {
            MapContainer mapContainer = context.getMapContainer(mapName);
            assert mapContainer != null : "null mapContainer";

            int estimatedRowCount = 0;
            QueryTargetDescriptor keyDescriptor = null;
            QueryTargetDescriptor valueDescriptor = null;

            for (PartitionContainer partitionContainer : context.getPartitionContainers()) {
                // Resolve sample.
                RecordStore<?> recordStore = partitionContainer.getExistingRecordStore(mapName);

                if (recordStore == null) {
                    continue;
                }

                Iterator<Map.Entry<Data, Record>> recordStoreIterator = recordStore.iterator();

                if (!recordStoreIterator.hasNext()) {
                    continue;
                }

                Map.Entry<Data, Record> entry = recordStoreIterator.next();

                InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

                MapSampleMetadata keyMetadata = MapSampleMetadataResolver.resolve(ss, entry.getKey(), true);
                MapSampleMetadata valueMetadata = MapSampleMetadataResolver.resolve(ss, entry.getValue().getValue(), false);
                keyDescriptor = keyMetadata.getDescriptor();
                valueDescriptor = valueMetadata.getDescriptor();

                estimatedRowCount = recordStore.size() * nodeEngine.getPartitionService().getPartitionCount();

                if (fields == null) {
                    fields = mergeMapFields(keyMetadata.getFields(), valueMetadata.getFields());
                }

                break;
            }

            if (fields == null) {
                // TODO: Throw an error here instead so that the user knows that resolution failed due to empty map.
                return null;
            }

            if (keyDescriptor == null) {
                keyDescriptor = new GenericQueryTargetDescriptor();
            }
            if (valueDescriptor == null) {
                valueDescriptor = new GenericQueryTargetDescriptor();
            }

            // Map fields to ordinals.
            Map<QueryPath, Integer> pathToOrdinalMap = new HashMap<>();

            for (int i = 0; i < fields.size(); i++) {
                pathToOrdinalMap.put(((MapTableField) fields.get(i)).getPath(), i);
            }

            // Resolve indexes.
            List<MapTableIndex> indexes = new ArrayList<>(mapContainer.getIndexDefinitions().size());

            for (IndexConfig indexConfig : mapContainer.getIndexDefinitions().values()) {
                List<Integer> indexFieldOrdinals = new ArrayList<>(indexConfig.getAttributes().size());

                for (String attribute : indexConfig.getAttributes()) {
                    QueryPath attributePath = QueryPath.create(attribute);

                    Integer ordinal = pathToOrdinalMap.get(attributePath);

                    if (ordinal == null) {
                        // No mapping for the field. Stop.
                        break;
                    }

                    indexFieldOrdinals.add(ordinal);
                }

                if (indexFieldOrdinals.isEmpty()) {
                    // Failed to resolve a prefix of the index, so it cannot be used in any query => skip.
                    break;
                }

                indexes.add(new MapTableIndex(mapName, indexConfig.getType(), indexFieldOrdinals));
            }

            // Resolve distribution field ordinal.
            int distributionFieldOrdinal = PartitionedMapTable.DISTRIBUTION_FIELD_ORDINAL_NONE;

            MapConfig mapConfig = mapContainer.getMapConfig();

            PartitioningStrategy partitioningStrategy =
                    context.getPartitioningStrategy(mapConfig.getName(), mapConfig.getPartitioningStrategyConfig());

            if (partitioningStrategy instanceof DeclarativePartitioningStrategy) {
                String field = ((DeclarativePartitioningStrategy) partitioningStrategy).getField();
                QueryPath fieldPath = new QueryPath(field, true);
                Integer fieldOrdinal = pathToOrdinalMap.get(fieldPath);

                if (fieldOrdinal != null) {
                    distributionFieldOrdinal = fieldOrdinal;
                }
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
                    ddlOptions
            );
        } catch (QueryException e) {
            throw e;
        } catch (Exception e) {
            throw QueryException.error("Failed to get metadata for IMap " + mapName + ": " + e.getMessage(), e);
        }
    }
}
