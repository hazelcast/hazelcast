/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.dataconnection.databasediscovery.impl;

import com.hazelcast.dataconnection.DataConnectionResource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.dataconnection.impl.JdbcDataConnection.OBJECT_TYPE_TABLE;

public class MSSQLDatabaseDiscovery {

    List<String> systemSchemaList = List.of("sys", "INFORMATION_SCHEMA");
    List<String> systemTableList = List.of("spt_", "MSreplication_options");

    public List<DataConnectionResource> listResources(Connection connection) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(
                     connection.getCatalog(),
                     null,
                     null,
                     new String[]{"TABLE", "VIEW"})) {
            List<DataConnectionResource> result = new ArrayList<>();
            while (tables.next()) {
                // Format DataConnectionResource name as catalog + schema+ + table_name
                String[] name = Stream.of(
                                tables.getString("TABLE_CAT"),
                                tables.getString("TABLE_SCHEM"),
                                tables.getString("TABLE_NAME")
                        )
                        .filter(Objects::nonNull)
                        .toArray(String[]::new);

                if (isSystemSchema(name[1]) || isSystemTable(name[2])) {
                    continue;
                }
                result.add(new DataConnectionResource(OBJECT_TYPE_TABLE, name));
            }
            return result;
        }
    }

    private boolean isSystemSchema(String schema) {
        return systemSchemaList.stream()
                .anyMatch(schema::startsWith);
    }

    private boolean isSystemTable(String tableName) {
        return systemTableList.stream()
                .anyMatch(tableName::startsWith);
    }
}
