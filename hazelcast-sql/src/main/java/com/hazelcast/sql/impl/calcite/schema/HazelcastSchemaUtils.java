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

import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.Table;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.AbstractMapTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for schema resolution.
 */
public final class HazelcastSchemaUtils {
    private HazelcastSchemaUtils() {
        // No-op.
    }

    /**
     * Creates the top-level catalog containing the given child schema.
     *
     * @param schema Schema.
     * @return Catalog.
     */
    public static HazelcastSchema createCatalog(Schema schema) {
        return new HazelcastSchema(
            Collections.singletonMap(QueryUtils.CATALOG, schema),
            Collections.emptyMap()
        );
    }

    /**
     * Construct a schema from the given table resolvers.
     * <p>
     * Currently we assume that all tables are resolved upfront by querying a table resolver. It works well for predefined
     * objects such as IMap and ReplicatedMap as well as external tables created by Jet. This approach will not work well
     * should we need a relaxed/dynamic object resolution at some point in future.
     *
     * @param tableResolvers Table resolver to be used to get the list of existing tables.
     * @return Top-level schema.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static HazelcastSchema createRootSchema(List<TableResolver> tableResolvers) {
        // Create tables.
        Map<String, Map<String, HazelcastTable>> tableMap = new HashMap<>();

        for (TableResolver tableResolver : tableResolvers) {
            for (Table table : tableResolver.getTables()) {
                HazelcastTable convertedTable = new HazelcastTable(
                    table,
                    createTableStatistic(table)
                );

                Map<String , HazelcastTable> schemaTableMap =
                    tableMap.computeIfAbsent(table.getSchemaName(), (k) -> new HashMap<>());

                schemaTableMap.putIfAbsent(table.getName(), convertedTable);
            }
        }

        // Create schemas.
        Map<String, Schema> schemaMap = new HashMap<>();

        for (Map.Entry<String, Map<String, HazelcastTable>> schemaEntry : tableMap.entrySet()) {
            String schemaName = schemaEntry.getKey();
            Map schemaTables = schemaEntry.getValue();

            HazelcastSchema schema = new HazelcastSchema(Collections.emptyMap(), schemaTables);

            schemaMap.put(schemaName, schema);
        }

        HazelcastSchema rootSchema = new HazelcastSchema(schemaMap, Collections.emptyMap());

        return createCatalog(rootSchema);
    }

    /**
     * Create Calcite {@link Statistic} object for the given table.
     * <p>
     * As neither IMDG core, nor Jet has dependency on the SQL module, we cannot get that object from the outside. Instead,
     * it should be created in the SQL module through {@code instanceof} checks (or similar).
     *
     * @param table Target table.
     * @return Statistics for the table.
     */
    private static Statistic createTableStatistic(Table table) {
        if (table instanceof AbstractMapTable) {
            return new MapTableStatistic(table.getStatistics().getRowCount());
        }

        throw new UnsupportedOperationException("Unsupported table type: " + table.getClass().getName());
    }

    /**
     * Prepares schema paths that will be used for search.
     *
     * @param currentSearchPaths Additional schema paths to be considered.
     * @return Schema paths to be used.
     */
    public static List<List<String>> prepareSearchPaths(
        List<List<String>> currentSearchPaths,
        List<TableResolver> tableResolvers
    ) {
        // Current search paths have the highest priority.
        List<List<String>> res = new ArrayList<>();

        if (currentSearchPaths != null) {
            res.addAll(currentSearchPaths);
        }

        // Then add paths from table resolvers.
        if (tableResolvers != null) {
            for (TableResolver tableResolver : tableResolvers) {
                List<List<String>> tableResolverSearchPaths = tableResolver.getDefaultSearchPaths();

                if (tableResolverSearchPaths != null) {
                    res.addAll(tableResolverSearchPaths);
                }
            }
        }

        // Add catalog scope.
        res.add(Collections.singletonList(QueryUtils.CATALOG));

        // Add top-level scope.
        res.add(Collections.emptyList());

        return res;
    }
}
