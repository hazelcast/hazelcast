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
import com.hazelcast.map.impl.MapContainer;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DeclarativePartitioningStrategy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.sql.impl.calcite.schema.statistic.StatisticProvider;
import com.hazelcast.sql.impl.calcite.schema.statistic.TableStatistic;
import com.hazelcast.sql.impl.schema.PartitionedMapSchemaResolver;
import com.hazelcast.sql.impl.schema.ReplicatedMapSchemaResolver;
import com.hazelcast.sql.impl.schema.SqlTableField;
import com.hazelcast.sql.impl.schema.SqlTableSchema;
import com.hazelcast.sql.impl.type.QueryDataType;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Utility classes for schema creation and management.
 */
public final class SchemaUtils {
    /** Default catalog for local Hazelcast cluster. */
    public static final String CATALOG = "hazelcast";

    /** Name of the implicit schema containing partitioned maps. */
    public static final String SCHEMA_NAME_PARTITIONED = "partitioned";

    /** Name of the implicit schema containing replicated maps. */
    public static final String SCHEMA_NAME_REPLICATED = "replicated";

    private SchemaUtils() {
        // No-op.
    }

    /**
     * Creates root schema for the given node engine.
     *
     * @return Root schema.
     */
    public static HazelcastSchema createRootSchema(
        NodeEngine nodeEngine,
        StatisticProvider statisticProvider
    ) {
        // Create partitioned and replicated schemas.
        HazelcastSchema partitionedSchema = preparePartitionedSchema(nodeEngine, statisticProvider);
        HazelcastSchema replicatedSchema = prepareReplicatedSchema(nodeEngine, statisticProvider);

        // Define predefined schemas.
        Map<String, Schema> schemaMap = new HashMap<>();
        schemaMap.put(SCHEMA_NAME_PARTITIONED, partitionedSchema);
        schemaMap.put(SCHEMA_NAME_REPLICATED, replicatedSchema);

        HazelcastSchema schema = new HazelcastSchema(schemaMap, Collections.emptyMap());

        // Create the root.
        return createCatalog(schema);
    }

    /**
     * Creates the top-level catalog containing the given child schema.
     *
     * @param schema Schema.
     * @return Catalog.
     */
    public static HazelcastSchema createCatalog(Schema schema) {
        return new HazelcastSchema(
            Collections.singletonMap(CATALOG, schema),
            Collections.emptyMap()
        );
    }

    @SuppressWarnings("rawtypes")
    private static HazelcastSchema preparePartitionedSchema(NodeEngine nodeEngine, StatisticProvider statisticProvider) {
        Map<String, Table> tableMap = new HashMap<>();

        PartitionedMapSchemaResolver resolver = new PartitionedMapSchemaResolver(nodeEngine);

        for (SqlTableSchema tableSchema : resolver.getTables()) {
            MapProxyImpl<?, ?> map = tableSchema.getTarget();

            long rowCount = statisticProvider.getRowCount(map);

            PartitioningStrategy strategy = map.getPartitionStrategy();

            String distributionField = null;

            if (strategy instanceof DeclarativePartitioningStrategy) {
                distributionField = ((DeclarativePartitioningStrategy) strategy).getField();
            }

            List<HazelcastTableIndex> indexes = getIndexes(map);

            Map<String, QueryDataType> fieldTypes = prepareFieldTypes(tableSchema);
            Map<String, String> fieldPaths = prepareFieldPaths(tableSchema);

            AbstractMapTable table = new PartitionedMapTable(
                SCHEMA_NAME_PARTITIONED,
                tableSchema.getName(),
                distributionField,
                indexes,
                tableSchema.getKeyDescriptor(),
                tableSchema.getValueDescriptor(),
                fieldTypes,
                fieldPaths,
                new TableStatistic(rowCount)
            );

            tableMap.put(tableSchema.getName(), table);
        }

        return new HazelcastSchema(Collections.emptyMap(), tableMap);
    }

    private static HazelcastSchema prepareReplicatedSchema(NodeEngine nodeEngine, StatisticProvider statisticProvider) {
        Map<String, Table> tableMap = new HashMap<>();

        ReplicatedMapSchemaResolver resolver = new ReplicatedMapSchemaResolver(nodeEngine);

        for (SqlTableSchema tableSchema : resolver.getTables()) {
            ReplicatedMapProxy<?, ?> map = tableSchema.getTarget();

            long rowCount = statisticProvider.getRowCount(map);

            Map<String, QueryDataType> fieldTypes = prepareFieldTypes(tableSchema);
            Map<String, String> fieldPaths = prepareFieldPaths(tableSchema);

            AbstractMapTable table = new ReplicatedMapTable(
                SCHEMA_NAME_REPLICATED,
                tableSchema.getName(),
                Collections.emptyList(),
                tableSchema.getKeyDescriptor(),
                tableSchema.getValueDescriptor(),
                fieldTypes,
                fieldPaths,
                new TableStatistic(rowCount)
            );

            tableMap.put(tableSchema.getName(), table);
        }

        return new HazelcastSchema(Collections.emptyMap(), tableMap);
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

    /**
     * Prepares schema paths that will be used for search.
     *
     * @param currentSchemaPaths Additional schema paths to be considered.
     * @return Schema paths to be used.
     */
    public static List<List<String>> prepareSchemaPaths(List<List<String>> currentSchemaPaths) {
        List<List<String>> res = new ArrayList<>();

        if (currentSchemaPaths != null) {
            res.addAll(currentSchemaPaths);
        }

        // Default schemas in the default catalog
        res.add(Arrays.asList("hazelcast", "partitioned"));
        res.add(Arrays.asList("hazelcast", "replicated"));

        // Default catalog
        res.add(Collections.singletonList("hazelcast"));

        // Current scope
        res.add(Collections.emptyList());

        return res;
    }
}
