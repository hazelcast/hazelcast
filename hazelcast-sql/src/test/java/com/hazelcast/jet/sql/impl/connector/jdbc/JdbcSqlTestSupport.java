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

package com.hazelcast.jet.sql.impl.connector.jdbc;

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

import static com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlConnector.OPTION_JDBC_URL;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * TestSupport for tests of JdbcSqlConnector
 */
public abstract class JdbcSqlTestSupport extends SqlTestSupport {

    protected static TestDatabaseProvider databaseProvider;

    protected static String dbConnectionUrl;
    protected static SqlService sqlService;

    public static void initialize(TestDatabaseProvider provider) {
        databaseProvider = provider;
        dbConnectionUrl = databaseProvider.createDatabase(JdbcSqlTestSupport.class.getName());
        initialize(2, null);

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

    static void insertItems(String tableName, int count) throws SQLException {
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
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_JDBC_URL + "'='" + dbConnectionUrl + "'"
                        + ")"
        );
    }

    protected static void createMapping(String tableName, String mappingName) {
        execute(
                "CREATE MAPPING " + mappingName
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME + ' '
                        + "OPTIONS ( "
                        + " '" + OPTION_JDBC_URL + "'='" + dbConnectionUrl + "'"
                        + ")"
        );
    }


    protected static void execute(String sql, Object... arguments) {
        requireNonNull(dbConnectionUrl);
        try (SqlResult ignored = sqlService.execute(sql, arguments)) {
            // empty try-with-resources
        }
    }

    /**
     * Assert the contents of a given table via Hazelcast SQL engine
     */
    protected static void assertRowsAnyOrder(String tableName, Row... rows) {
        assertRowsAnyOrder("SELECT * FROM " + tableName, Arrays.asList(rows));
    }

    /**
     * Assert the contents of a given table directly via JDBC
     */
    protected static void assertJdbcRowsAnyOrder(String tableName, Row... rows) {
        List<Row> actualRows = jdbcRows(tableName);
        assertThat(actualRows).containsExactlyInAnyOrderElementsOf(Arrays.asList(rows));
    }

    @Nonnull
    protected static List<Row> jdbcRows(String tableName) {
        List<Row> rows = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("SELECT * FROM " + tableName);
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
