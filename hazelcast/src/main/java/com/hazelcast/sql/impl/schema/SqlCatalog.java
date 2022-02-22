/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl.schema;

import com.hazelcast.sql.impl.QueryUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Schema that is used for the duration of a query.
 */
public class SqlCatalog {

    private final Map<String, Map<String, Table>> schemas;

    public SqlCatalog(List<TableResolver> tableResolvers) {
        // Populate schemas and tables.
        schemas = new HashMap<>();

        Map<String, Set<Table>> tableConflicts = new HashMap<>();

        for (TableResolver tableResolver : tableResolvers) {
            Collection<Table> tables = tableResolver.getTables();

            for (List<String> searchPath : tableResolver.getDefaultSearchPaths()) {
                assert searchPath.size() == 2 && searchPath.get(0).equals(QueryUtils.CATALOG) : searchPath;

                schemas.putIfAbsent(searchPath.get(1), new HashMap<>());
            }

            for (Table table : tables) {
                String schemaName = table.getSchemaName();
                String tableName = table.getSqlName();

                Table oldTable = schemas
                        .computeIfAbsent(schemaName, key -> new HashMap<>())
                        .putIfAbsent(tableName, table);

                if (oldTable == null) {
                    tableConflicts.computeIfAbsent(tableName, key -> new HashSet<>()).add(table);
                }
            }
        }

        // Add conflict information to tables
        for (Set<Table> tableConflict : tableConflicts.values()) {
            if (tableConflict.size() == 1) {
                // No conflict.
                continue;
            }

            Set<String> conflictingSchemas = new HashSet<>(tableConflict.size());

            for (Table table : tableConflict) {
                conflictingSchemas.add(table.getSchemaName());
            }

            for (Table table : tableConflict) {
                table.setConflictingSchemas(conflictingSchemas);
            }
        }
    }

    public Map<String, Map<String, Table>> getSchemas() {
        return schemas;
    }
}
