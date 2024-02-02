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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.Column.col;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.INT;
import static com.hazelcast.test.jdbc.TestDatabaseRecordProvider.ColumnType.STRING;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

public class JdbcObjectProvider implements TestDatabaseRecordProvider {

    protected JdbcDatabaseProvider<?> databaseProvider;

    public JdbcObjectProvider(JdbcDatabaseProvider<?> databaseProvider) {
        this.databaseProvider = databaseProvider;
    }

    @Override
    public TestDatabaseProvider provider() {
        return databaseProvider;
    }

    @Override
    public ObjectSpec createObject(String objectName, boolean useQuotedNames) {
        String idName = useQuotedNames ? databaseProvider.quote("person-id") : "id";
        var spec = new ObjectSpec(objectName, col(idName, INT, true), col("name", STRING));
        createObject(spec);
        return spec;
    }

    public void createSchema(String schemaName) {
        String query = createSchemaQuery(schemaName);
        try (Connection conn = DriverManager.getConnection(databaseProvider.url());
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(query);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public String createSchemaQuery(String schemaName) {
        return "CREATE SCHEMA IF NOT EXISTS " + databaseProvider.quote(schemaName);
    }

    @Override
    public void createObject(ObjectSpec spec) {
        String objectName = spec.name;
        List<Column> columns = spec.columns;
        String columnString = columns.stream().map(c -> c.name + " " + resolveType(c.type) + pk(c)).collect(joining(", "));
        String sql = createSql(objectName, columnString);
        try (Connection conn = DriverManager.getConnection(databaseProvider.url());
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected String pk(Column c) {
        return c.primaryKey ? " PRIMARY KEY" : "";
    }

    protected String resolveType(ColumnType type) {
        switch (type) {
            case INT: return "INT";
            case STRING: return "VARCHAR(100)";
            default: throw new UnsupportedOperationException("type " + type + " is not supported in MongoDatabaseProvider");
        }
    }

    protected String createSql(String objectName, String... columns) {
        return "CREATE TABLE " + objectName + " (" + String.join(", ", columns) + ")";
    }

    @Override
    public void insertItems(ObjectSpec spec, int count) {
        var objectName = spec.name;
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
                        case INT: stmt.setInt(col + 1, i + col); break;
                        case STRING: stmt.setString(col + 1, String.format("%s-%d", column.name, i)); break;
                        default: throw new UnsupportedOperationException();
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
    public void assertRows(String tableName, List<List<Object>> rows) {
        List<List<Object>> actualRows = jdbcRows("SELECT * FROM " + tableName, databaseProvider.url(), null);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rows);
    }

    @Override
    public void assertRows(String tableName, List<Class<?>> columnType, List<List<Object>> rows) {
        List<List<Object>> actualRows = jdbcRows("SELECT * FROM " + tableName, databaseProvider.url(), columnType);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(rows);
    }

    public static List<List<Object>> jdbcRows(String query, String connectionUrl, List<Class<?>> columnType) {
        List<List<Object>> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                Object[] values = new Object[resultSet.getMetaData().getColumnCount()];
                for (int i = 0; i < values.length; i++) {
                    if (columnType == null) {
                        values[i] = resultSet.getObject(i + 1);
                    } else {
                        values[i] = resultSet.getObject(i + 1, columnType.get(i));
                    }
                }
                rows.add(Arrays.asList(values));
            }
            return rows;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }

    public void createTable(String tableName, String... columns) {
        String sql = "CREATE TABLE " + databaseProvider.quote(tableName) + " ("
                + Stream.of(columns)
                .map(s -> {
                    int spaceIndex = s.indexOf(' ');
                    return databaseProvider.quote(s.substring(0, spaceIndex)) + s.substring(spaceIndex);
                }).collect(joining(", "))
                + ")";
        try (Connection conn = DriverManager.getConnection(databaseProvider.url());
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
