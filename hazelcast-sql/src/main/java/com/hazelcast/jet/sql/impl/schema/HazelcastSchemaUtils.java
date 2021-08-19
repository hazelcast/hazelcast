/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sql.impl.schema;

import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.Table;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;

import java.util.Collections;
import java.util.HashMap;
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
     * @return Top-level schema.
     */
    public static HazelcastSchema createRootSchema(SqlCatalog catalog) {
        // Create schemas.
        Map<String, Schema> schemaMap = new HashMap<>();

        for (Map.Entry<String, Map<String, Table>> currentSchemaEntry : catalog.getSchemas().entrySet()) {
            String schemaName = currentSchemaEntry.getKey();

            Map<String, org.apache.calcite.schema.Table> schemaTables = new HashMap<>();

            for (Map.Entry<String, Table> tableEntry : currentSchemaEntry.getValue().entrySet()) {
                String tableName = tableEntry.getKey();
                Table table = tableEntry.getValue();

                HazelcastTable convertedTable = new HazelcastTable(
                        table,
                        createTableStatistic(table)
                );

                schemaTables.put(tableName, convertedTable);
            }

            HazelcastSchema currentSchema = new HazelcastSchema(Collections.emptyMap(), schemaTables);

            schemaMap.put(schemaName, currentSchema);
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
        return new HazelcastTableStatistic(table.getStatistics().getRowCount());
    }
}
