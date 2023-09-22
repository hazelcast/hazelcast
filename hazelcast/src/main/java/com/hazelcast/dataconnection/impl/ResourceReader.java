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

package com.hazelcast.dataconnection.impl;

import com.hazelcast.dataconnection.DataConnectionResource;
import com.hazelcast.jet.function.TriPredicate;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import static com.hazelcast.dataconnection.impl.JdbcDataConnection.OBJECT_TYPE_TABLE;

class ResourceReader {

    private String catalog;

    private TriPredicate<String, String, String> excludePredicate = (catalog, schema, table) -> false;

    public ResourceReader withCatalog(String schema) {
        this.catalog = schema;
        return this;
    }

    public ResourceReader exclude(TriPredicate<String, String, String> predicate) {
        this.excludePredicate = predicate;
        return this;
    }

    public List<DataConnectionResource> listResources(Connection connection) throws SQLException {
        try (ResultSet tables = connection.getMetaData().getTables(
                catalog,
                null,
                null,
                new String[]{"TABLE", "VIEW"})) {

            List<DataConnectionResource> result = new ArrayList<>();
            while (tables.next()) {
                String catalogName = tables.getString("TABLE_CAT");
                String schemaName = tables.getString("TABLE_SCHEM");
                String tableName = tables.getString("TABLE_NAME");

                if (excludePredicate.test(catalogName, schemaName, tableName)) {
                    continue;
                }

                // Format DataConnectionResource name as catalog + schema + table_name
                String[] name = Stream.of(
                                catalogName,
                                schemaName,
                                tableName
                        )
                        .filter(Objects::nonNull)
                        .toArray(String[]::new);

                result.add(new DataConnectionResource(OBJECT_TYPE_TABLE, name));
            }
            return result;
        }
    }
}
