/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.test.jdbc;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.IntStream;

import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.Column.col;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.INT;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.STRING;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class OracleObjectProvider extends JdbcObjectProvider {
    public OracleObjectProvider(JdbcDatabaseProvider<?> databaseProvider) {
        super(databaseProvider);
    }

    @Override
    public String createSchemaQuery(String schemaName) {
        return "CREATE USER " + databaseProvider.quote(schemaName) + " IDENTIFIED BY \"password\"\n\n"
                + "GRANT UNLIMITED TABLESPACE TO " + databaseProvider.quote(schemaName);
    }

    @Override
    public void createObject(ObjectSpec spec) {
        String objectName = databaseProvider.quote(spec.name);
        List<Column> columns = spec.columns;
        String columnString = columns.stream().map(c -> databaseProvider.quote(c.name) + " " + resolveType(c.type) + pk(c)).collect(joining(", "));
        String sql = createSql(objectName, columnString);
        try (Connection conn = DriverManager.getConnection(databaseProvider.url());
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void insertItems(ObjectSpec spec, int count) {
        var objectName = databaseProvider.quote(spec.name);
        int start = 0;
        int end = start + count;

        int columnCount = spec.columns.size();
        String questionMarks = IntStream.range(0, columnCount).mapToObj(i -> "?").collect(joining(", "));
        String sql = String.format("INSERT INTO %s VALUES(%s)", objectName, questionMarks);

        try (Connection conn = DriverManager.getConnection(databaseProvider.url());
             PreparedStatement stmt = conn.prepareStatement(sql)
        ) {
            for (int i = start; i < end; i++) {
                int col = 0;
                for (Column column : spec.columns) {
                    switch (column.type) {
                        case INT:
                            stmt.setInt(col + 1, i + col);
                            break;

                        case STRING:
                            stmt.setString(col + 1, String.format("%s-%d", column.name, i));
                            break;

                        default:
                            throw new UnsupportedOperationException();
                    }
                    col++;
                }
                stmt.addBatch();
                stmt.clearParameters();
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ObjectSpec createObject(String objectName, boolean useQuotedNames) {
        String idName = useQuotedNames ? "person-id" : "id";
        var spec = new ObjectSpec(objectName, col(idName, INT, true), col("name", STRING));
        createObject(spec);
        return spec;
    }

    @Override
    public void assertRows(String tableName, List<List<Object>> rows) {
        List<List<Object>> actualRows = jdbcRows("SELECT * FROM " + databaseProvider.quote(tableName), databaseProvider.url(), null);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rows);
    }

    @Override
    public void assertRows(String tableName, List<Class<?>> columnType, List<List<Object>> rows) {
        List<List<Object>> actualRows = jdbcRows("SELECT * FROM " + databaseProvider.quote(tableName) , databaseProvider.url(), columnType);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rows);
    }

    @Override
    protected String resolveType(ColumnType type) {
        switch (type) {
            case INT: return "NUMBER(9)";
            case STRING: return "VARCHAR(100)";
            default: throw new UnsupportedOperationException("type " + type + " is not supported in OracleDatabaseProvider");
        }
    }
}
