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

package com.hazelcast.sql.impl.calcite.schema;

import com.hazelcast.config.IndexConfig;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.statistics.StatisticProvider;
import com.hazelcast.sql.impl.calcite.statistics.TableStatistics;
import com.hazelcast.sql.impl.schema.SqlSchemaResolver;
import com.hazelcast.sql.impl.schema.SqlTableField;
import com.hazelcast.sql.impl.schema.SqlTableSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.hazelcast.sql.impl.schema.SqlSchemaResolver.SCHEMA_NAME_PARTITIONED;
import static com.hazelcast.sql.impl.schema.SqlSchemaResolver.SCHEMA_NAME_REPLICATED;

/**
 * Utility classes for schema creation.
 */
public final class SchemaUtils {
    private SchemaUtils() {
        // No-op.
    }

    /**
     * Creates root schema for the given node engine.
     *
     * @param nodeEngine Node engine.
     * @return Root schema.
     */
    public static HazelcastSchema createRootSchema(
        NodeEngine nodeEngine,
        StatisticProvider statisticProvider,
        SqlSchemaResolver schemaResolver
    ) {
        // Create partitioned and replicated schemas.
        List<HazelcastTable> partitionedTables = prepareSchemaTables(nodeEngine, statisticProvider, schemaResolver, true);
        List<HazelcastTable> replicatedTables = prepareSchemaTables(nodeEngine, statisticProvider, schemaResolver, false);

        // Create root schema.
        Map<String, Schema> schemaMap = new HashMap<>();
        Map<String, Table> topTableMap = new HashMap<>();

        processTables(schemaMap, topTableMap, partitionedTables);
        processTables(schemaMap, topTableMap, replicatedTables);

        return new HazelcastSchema(schemaMap, topTableMap);
    }

    private static void processTables(
        Map<String, Schema> schemaMap,
        Map<String, Table> topTableMap,
        List<HazelcastTable> tables
    ) {
        for (HazelcastTable table : tables) {
            String schemaName = table.getSchemaName();

            HazelcastSchema schema = (HazelcastSchema) schemaMap.get(schemaName);

            if (schema == null) {
                // TODO: Ugly, refactor it.
                schema = new HazelcastSchema(new HashMap<>());

                schemaMap.put(schemaName, schema);
            }

            // TODO: What to do in case the table with this name is already registered? Print a warning?
            schema.getTableMap().putIfAbsent(table.getName(), table);

            if (schemaName.equals(SCHEMA_NAME_PARTITIONED) || schemaName.equals(SCHEMA_NAME_REPLICATED)) {
                // TODO: What to do in case the table with this name is already registered? Print a warning?
                topTableMap.putIfAbsent(table.getName(), table);
            }
        }
    }

    /**
     * Prepare the list of available tables.
     *
     * @param nodeEngine Node engine.
     * @param partitioned {@code True} to prepare the list of partitioned tables, {@code false} to prepare the list
     *     if replicated tables.
     * @return List of tables.
     */
    @SuppressWarnings("rawtypes")
    private static List<HazelcastTable> prepareSchemaTables(
        NodeEngine nodeEngine,
        StatisticProvider statisticProvider,
        SqlSchemaResolver schemaResolver,
        boolean partitioned
    ) {
        String serviceName = partitioned ? MapService.SERVICE_NAME : ReplicatedMapService.SERVICE_NAME;

        Collection<String> mapNames = nodeEngine.getProxyService().getDistributedObjectNames(serviceName);

        List<HazelcastTable> res = new ArrayList<>();

        for (String mapName : mapNames) {
            DistributedObject map = nodeEngine.getProxyService().getDistributedObject(
                serviceName,
                mapName,
                nodeEngine.getLocalMember().getUuid()
            );

            SqlTableSchema tableSchema = schemaResolver.resolve(map);

            if (tableSchema == null) {
                // Skip empty maps.
                continue;
            }

            long rowCount = statisticProvider.getRowCount(map);

            String distributionField;
            List<HazelcastTableIndex> indexes;

            if (partitioned) {
                MapProxyImpl<?, ?> map0 = (MapProxyImpl) map;
                PartitioningStrategy strategy = map0.getPartitionStrategy();

                if (strategy instanceof DeclarativePartitioningStrategy) {
                    distributionField = ((DeclarativePartitioningStrategy) strategy).getField();
                } else {
                    distributionField = null;
                }


                indexes = getIndexes(map0);
            } else {
                distributionField = null;
                indexes = null;
            }

            Map<String, QueryDataType> fieldTypes = prepareFieldTypes(tableSchema);
            Map<String, String> fieldPaths = prepareFieldPaths(tableSchema);

            HazelcastTable table = new HazelcastTable(
                tableSchema.getSchema(),
                mapName,
                partitioned,
                distributionField,
                indexes,
                tableSchema.getKeyDescriptor(),
                tableSchema.getValueDescriptor(),
                fieldTypes,
                fieldPaths,
                new TableStatistics(rowCount)
            );

            res.add(table);
        }

        return res;
    }

    private static Map<String, QueryDataType> prepareFieldTypes(SqlTableSchema tableSchema) {
        Map<String, QueryDataType> res = new HashMap<>();

        for (SqlTableField field : tableSchema.getFields()) {
            res.put(field.getName(), field.getType());
        }

        return res;
    }

    private static Map<String, String> prepareFieldPaths(SqlTableSchema tableSchema) {
        Map<String, String> res = new HashMap<>();

        for (SqlTableField field : tableSchema.getFields()) {
            res.put(field.getName(), field.getPath());
        }

        return res;
    }

    /**
     * Get indexes from the map config.
     *
     * @param map Map.
     * @return Indexes.
     */
    // TODO: Move this to schema resolver
    private static List<HazelcastTableIndex> getIndexes(MapProxyImpl<?, ?> map) {
        MapContainer mapContainer = map.getMapServiceContext().getMapContainer(map.getName());

        Collection<IndexConfig> indexConfigs = mapContainer.getIndexDefinitions().values();

        List<HazelcastTableIndex> res = new ArrayList<>(indexConfigs.size());

        for (IndexConfig indexConfig : indexConfigs) {
            boolean duplicate = false;

            for (HazelcastTableIndex index : res) {
                //
                if (Objects.equals(indexConfig.getType(), index.getType())
                    && Objects.equals(indexConfig.getAttributes(), index.getAttributes())) {
                    duplicate = true;

                    break;
                }
            }

            if (duplicate) {
                continue;
            }

            res.add(new HazelcastTableIndex(indexConfig.getName(), indexConfig.getType(), indexConfig.getAttributes()));
        }

        return res;
    }
}
