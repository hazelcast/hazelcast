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
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.jet.sql.SqlTestSupport;
import com.hazelcast.jet.test.IgnoreInJenkinsOnWindows;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.test.jdbc.TestDatabaseProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import javax.annotation.Nonnull;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.hazelcast.test.DockerTestUtil.assumeDockerEnabled;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TestSupport for tests of JdbcSqlConnector
 */
@Category(IgnoreInJenkinsOnWindows.class)
public abstract class JdbcSqlTestSupport extends SqlTestSupport {

    protected static final String TEST_DATABASE_REF = "testDatabaseRef";

    protected static TestDatabaseProvider databaseProvider;

    protected static String dbConnectionUrl;
    protected static SqlService sqlService;

    @BeforeClass
    public static void checkDockerEnabled() {
        assumeDockerEnabled();
    }

    public static void initialize(TestDatabaseProvider provider) {
        Config config = smallInstanceConfig();
        config.getJetConfig().getDefaultEdgeConfig().setQueueSize(16);
        initialize(provider, config);
    }

    public static void initialize(TestDatabaseProvider provider, Config config) {
        databaseProvider = provider;
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", dbConnectionUrl);
        config.addDataConnectionConfig(
                new DataConnectionConfig(TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );
        initialize(2, config);
        sqlService = instance().getSql();
    }

    @AfterClass
    public static void afterClass() {
        if (databaseProvider != null) {
            databaseProvider.shutdown();
            databaseProvider = null;
            dbConnectionUrl = null;
        }
    }

    @Nonnull
    protected static String randomTableName() {
        return "table_" + randomName();
    }

    /**
     * Quotes given compound identifier using the {@link #databaseProvider}
     */
    protected static String quote(String... parts) {
        return databaseProvider.quote(parts);
    }

    /**
     * Creates a table with id VARCHAR, name VARCHAR columns
     * Quotes the given tableName.
     */
    public static void createTable(String tableName) throws SQLException {
        createTableNoQuote(quote(tableName), quote("id") + " INT PRIMARY KEY", quote("name") + " VARCHAR(100)");
    }

    /**
     * Creates table with given column definitions
     * Quotes given table name and column names - first token from each column definition
     */
    public static void createTable(String tableName, String... columns) throws SQLException {
        executeJdbc("CREATE TABLE " + quote(tableName) + " ("
                + Stream.of(columns)
                        .map(s -> {
                            int spaceIndex = s.indexOf(' ');
                            return quote(s.substring(0, spaceIndex)) + s.substring(spaceIndex);
                        }).collect(joining(", "))
                + ")"
        );
    }

    /**
     * Creates table with id INT, name VARCHAR columns
     * Does not quote the given tableName.
     */
    public static void createTableNoQuote(String tableName) throws SQLException {
        createTableNoQuote(tableName, quote("id") + " INT PRIMARY KEY", quote("name") + " VARCHAR(100)");
    }

    /**
     * Creates table with given column definitions
     * Does not quote the given tableName nor the column names.
     */
    public static void createTableNoQuote(String tableName, String... columns) throws SQLException {
        executeJdbc("CREATE TABLE " + tableName + " (" + String.join(", ", columns) + ")");
    }

    public static void executeJdbc(String sql) throws SQLException {
        requireNonNull(dbConnectionUrl, "dbConnectionUrl must be set");

        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            String[] parts = sql.split("\n\n");
            for (String part : parts) {
                stmt.execute(part);
            }
        }
    }

    public static void executeJdbcWithQuotes(String sql, String tableName) throws SQLException {
        requireNonNull(dbConnectionUrl, "dbConnectionUrl must be set");
        //Put the table name in quotations
        String[] substrings = sql.split(" ");
        String finalSql = "";
        for (String str : substrings) {
            if (str.equals(tableName)) {
                finalSql += (quote(str) + " ") ;
            } else {
                finalSql += (str + " ");
            }
        }
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute(finalSql);
        }
    }

    public static void insertItems(String tableName, int start, int count) throws SQLException {
        int end = start + count;
        String sql = String.format("INSERT INTO %s VALUES(?, ?)", tableName);

        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             PreparedStatement stmt = conn.prepareStatement(sql)
        ) {

            for (int i = start; i < end; i++) {
                stmt.setInt(1, i);
                stmt.setString(2, String.format("name-%d", i));
                stmt.addBatch();
                stmt.clearParameters();
            }
            stmt.executeBatch();
        }
    }

    public static void insertItems(String tableName, int count) throws SQLException {
        insertItems(quote(tableName), 0, count);
    }

    public static void insertItemsNoQuote(String tableName, int count) throws SQLException {
        insertItems(tableName, 0, count);
    }

    protected static void createMapping(String tableName) {
        execute(
                "CREATE MAPPING \"" + tableName + "\" ("
                + " id INT, "
                + " name VARCHAR "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
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
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );
    }

    protected static void createJdbcMappingUsingDataConnection(String name, String dataConnection) {
        try (SqlResult result = instance().getSql().execute("CREATE OR REPLACE MAPPING " + name +
                                                            " DATA CONNECTION " + quoteName(dataConnection) + "\n"
        )) {
            assertThat(result.updateCount()).isZero();
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

    protected static void assertJdbcRowsAnyOrder(String tableName, List<Class<?>> columnTypes, Row... rows) {
        List<Row> actualRows = jdbcRowsTable(tableName, columnTypes);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    protected static void assertJdbcQueryRowsAnyOrder(String query, Row... rows) {
        List<Row> actualRows = jdbcRows(query);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    protected static void assertJdbcQueryRowsAnyOrder(String query, List<Class<?>> columnTypes, Row... rows) {
        List<Row> actualRows = jdbcRows(query, columnTypes);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    protected static List<Row> jdbcRowsTable(String tableName) {
        return jdbcRows("SELECT * FROM " + quote(tableName));
    }

    protected static List<Row> jdbcRowsTable(String tableName, List<Class<?>> columnType) {
        return jdbcRows("SELECT * FROM " + quote(tableName), columnType);
    }

    protected static void assertJdbcRowsAnyOrderNoQuote(String tableName, List<Class<?>> columnTypes, Row... rows) {
        List<Row> actualRows = jdbcRowsTableNoQuote(tableName, columnTypes);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    protected static List<Row> jdbcRowsTableNoQuote(String tableName, List<Class<?>> columnType) {
        return jdbcRows("SELECT * FROM " + tableName, columnType);
    }

    @Nonnull
    protected static List<Row> jdbcRows(String query) {
        return jdbcRows(query, dbConnectionUrl);
    }

    protected static List<Row> jdbcRows(String query, List<Class<?>> columnType) {
        return jdbcRows(query, dbConnectionUrl, columnType);
    }

    public static List<Row> jdbcRows(String query, String connectionUrl) {
        return jdbcRows(query, connectionUrl, null);
    }

    public static List<Row> jdbcRows(String query, String connectionUrl, List<Class<?>> columnTypes) {
        List<Row> rows = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl);
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                Object[] values = new Object[resultSet.getMetaData().getColumnCount()];
                for (int i = 0; i < values.length; i++) {
                    if (columnTypes == null) {
                        values[i] = resultSet.getObject(i + 1);
                    } else {
                        values[i] = resultSet.getObject(i + 1, columnTypes.get(i));
                    }
                }
                rows.add(new Row(values));
            }
            return rows;
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        }
    }
}
