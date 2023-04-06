/*
 * Copyright 2023 Hazelcast Inc.
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

package com.hazelcast.jet.sql.impl.connector.jdbc;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataLinkConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.AfterClass;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_DATA_LINK_NAME;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TestSupport for tests of JdbcSqlConnector
 */
public abstract class JdbcSqlTestSupport extends SqlTestSupport {

    protected static final String TEST_DATABASE_REF = "test-database-ref";

    protected static TestDatabaseProvider databaseProvider;

    protected static String dbConnectionUrl;
    protected static SqlService sqlService;

    public static void initialize(TestDatabaseProvider provider) {
        initialize(provider, smallInstanceConfig());
    }

    public static void initialize(TestDatabaseProvider provider, Config config) {
        databaseProvider = provider;
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", dbConnectionUrl);
        config.addDataLinkConfig(
                new DataLinkConfig(TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );
        initialize(2, config);
        sqlService = instance().getSql();
    }

    @AfterClass
    public static void afterClass() throws SQLException {
        if (dbConnectionUrl != null) {
            databaseProvider.shutdown();
            databaseProvider = null;
            dbConnectionUrl = null;
        }
    }

    @Nonnull
    protected static String randomTableName() {
        return "table_" + randomName();
    }

    protected String quote(String... parts) {
        return Arrays.stream(parts)
                     .map(part -> '\"' + part.replaceAll("\"", "\"\"") + '\"')
                     .collect(joining("."));
    }

    /**
     * Creates table with id INT, name VARCHAR columns
     */
    public static void createTable(String tableName) throws SQLException {
        createTable(tableName, "id INT PRIMARY KEY", "name VARCHAR(100)");
    }

    public static void createTable(String tableName, String... columns) throws SQLException {
        executeJdbc("CREATE TABLE " + tableName + " (" + String.join(", ", columns) + ")");
    }

    static void createTableWithAllTypes(String tableName) throws SQLException {
        executeJdbc("CREATE TABLE " + tableName + " (" +
                "v VARCHAR(100)," +
                "b BOOLEAN, " +
                "ti TINYINT, " +
                "si SMALLINT, " +
                "i INTEGER, " +
                "bi BIGINT, " +
                "dc DECIMAL, " +
                "r REAL, " +
                "dbl DOUBLE, " +
                "tm TIME, " +
                "dt DATE, " +
                "ts TIMESTAMP, " +
                "tstz TIMESTAMP WITH TIME ZONE" +
                ")"
        );
    }

    public static void executeJdbc(String sql) throws SQLException {
        requireNonNull(dbConnectionUrl, "dbConnectionUrl must be set");

        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(sql);
        }
    }

    public static void insertItems(String tableName, int count) throws SQLException {
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            for (int i = 0; i < count; i++) {
                stmt.execute(String.format("INSERT INTO " + tableName + " VALUES(%d, 'name-%d')", i, i));
            }
        }
    }

    protected static void createMapping(String tableName) {
        execute(
                "CREATE MAPPING \"" + tableName + "\" ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );
    }

    protected static void createMapping(String tableName, String mappingName) {
        execute(
                "CREATE MAPPING \"" + mappingName + "\""
                        + " EXTERNAL NAME " + tableName + " "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                        + ")"
        );
    }

    protected static void createJdbcMappingUsingDataLink(String name, String dataLink) {
        try (SqlResult result = instance().getSql().execute("CREATE OR REPLACE MAPPING " + name +
                " DATA LINK " + quoteName(dataLink) + "\n"
                + "OPTIONS ( "
                + " '" + OPTION_DATA_LINK_NAME + "'='" + TEST_DATABASE_REF + "'"
                + ")"
        )) {
            assertThat(result.updateCount()).isEqualTo(0);
        }
    }

    protected static void execute(String sql, Object... arguments) {
        requireNonNull(dbConnectionUrl);
        try (SqlResult ignored = sqlService.execute(sql, arguments)) {
            // empty try-with-resources
        }
    }

    /**
     * Assert the contents of a given table directly via JDBC
     */
    protected static void assertJdbcRowsAnyOrder(String tableName, Row... rows) {
        List<Row> actualRows = jdbcRowsTable(tableName);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    protected static List<Row> jdbcRowsTable(String tableName) {
        return jdbcRows("SELECT * FROM " + tableName);
    }

    @Nonnull
    protected static List<Row> jdbcRows(String query) {
        List<Row> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(query);
            ResultSet resultSet = stmt.getResultSet();
            while (resultSet.next()) {
                Object[] values = new Object[resultSet.getMetaData().getColumnCount()];
                for (int i = 0; i < values.length; i++) {
                    values[i] = resultSet.getObject(i + 1);
                }
                rows.add(new Row(values));
            }
            return rows;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
