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

import com.google.common.collect.ImmutableList;
import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.dataconnection.impl.InternalDataConnectionService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlColumnMetadata;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlRowMetadata;
import com.hazelcast.test.jdbc.H2DatabaseProvider;
import com.hazelcast.test.jdbc.MySQLDatabaseProvider;
import com.hazelcast.test.jdbc.PostgresDatabaseProvider;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.hazelcast.jet.sql.SqlJsonTestSupport.jsonArray;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.assertj.core.util.Lists.newArrayList;

public class MappingJdbcSqlConnectorTest extends JdbcSqlTestSupport {

    private static final String LE = System.lineSeparator();

    protected String tableName;

    @BeforeClass
    public static void beforeClass() {
        initialize(new H2DatabaseProvider());
    }

    @Before
    public void setUp() throws Exception {
        tableName = randomTableName();
    }

    @Test
    public void createMappingWithExternalTableName() throws Exception {
        createTable(tableName);

        String mappingName = "mapping_" + randomName();
        createMapping(tableName, mappingName);

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(mappingName))
        );
    }

    @Test
    public void createMappingWithExternalSchemaAndTableName() throws Exception {
        String schemaName = "schema1";
        executeJdbc(databaseProvider.createSchemaQuery(schemaName));
        createTableNoQuote(quote(schemaName, tableName));

        String mappingName = "mapping_" + randomName();
        createMapping("\"schema1\".\"" + tableName + '\"', mappingName);

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(mappingName))
        );

        String expectedMappingQuery = "CREATE OR REPLACE EXTERNAL MAPPING \"hazelcast\".\"public\"" +
                ".\"" + mappingName + "\" EXTERNAL NAME \"schema1\"" +
                ".\"" + tableName + "\" (" + LE +
                "  \"id\" INTEGER," + LE +
                "  \"name\" VARCHAR" + LE +
                ")" + LE +
                "DATA CONNECTION \"testDatabaseRef\"" + LE +
                "OBJECT TYPE \"Table\"";
        assertRowsAnyOrder("SELECT GET_DDL('relation', '" + mappingName + "')",
                ImmutableList.of(new Row(expectedMappingQuery)));
    }

    @Test
    public void createMappingWithExternalTableNameTooManyComponents() throws Exception {
        createTable(tableName); // TODO this line can be removed?

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME \"aaaa\".\"bbbb\".\"cccc\".\"dddd\" "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid external name \"aaaa\".\"bbbb\".\"cccc\".\"dddd\"");
    }

    @Test
    public void createMappingWithExternalTableNameTooManyComponentsNoQuotes() throws Exception {
        createTable(tableName); // TODO this line can be removed?

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME aaaa.bbbb.cccc.dddd "
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Invalid external name \"aaaa\".\"bbbb\".\"cccc\".\"dddd\"");
    }

    @Test
    public void createMappingWithoutDataConnection() {
        tableName = randomTableName();

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "TYPE " + JdbcSqlConnector.TYPE_NAME
                )
        ).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("You must provide data connection when using the Jdbc connector");
    }

    @Test
    public void createMappingTableDoesNotExist() {
        tableName = randomTableName();
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class);


        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingNoColumnsTableDoesNotExist() {
        tableName = randomTableName();
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " DATA CONNECTION " + TEST_DATABASE_REF
        )).isInstanceOf(HazelcastSqlException.class);

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingWithExternalFieldName() throws Exception {
        createTable(tableName);

        execute("CREATE MAPPING " + tableName
                + " ("
                + " id INT, "
                + " fullName VARCHAR EXTERNAL NAME name "
                + ") "
                + "DATA CONNECTION " + TEST_DATABASE_REF
        );

        assertRowsAnyOrder("SHOW MAPPINGS",
                newArrayList(new Row(tableName))
        );

        insertItems(tableName, 1);
        assertRowsAnyOrder(
                "SELECT id,fullName FROM " + tableName,
                newArrayList(new Row(0, "name-0"))
        );
    }

    @Test
    public void createMappingFieldDoesNotExist() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " ("
                        + " id INT, "
                        + " fullName VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not resolve field with name fullName");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingExternalFieldDoesNotExist() throws Exception {
        tableName = randomTableName();
        try (Connection conn = DriverManager.getConnection(dbConnectionUrl);
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("CREATE TABLE " + quote(tableName) + " (" + quote("id") + " INT PRIMARY KEY, " + quote("name") + " VARCHAR(10))");
        }

        assertThatThrownBy(() ->
                sqlService.executeUpdate("CREATE MAPPING " + tableName
                        + " ("
                        + " id INT, "
                        + " fullName VARCHAR EXTERNAL NAME myName "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not resolve field with external name myName");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void createMappingFieldTypesDoNotMatch() throws Exception {
        createTable(tableName);

        assertThatThrownBy(() ->
                execute("CREATE MAPPING " + tableName
                        + " ("
                        + " id BOOLEAN, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).isInstanceOf(HazelcastSqlException.class)
                // Oracle converts INT to DECIMAL, doesn't really matter, it is still not convertible
                .hasMessageMatching("Type BOOLEAN of field id does not match db type (INTEGER|DECIMAL)");

        assertRowsAnyOrder("SHOW MAPPINGS",
                emptyList()
        );
    }

    @Test
    public void when_createMappingWithImplicitFieldTypesDefinition_then_orderIsPreserved() throws Exception {
        createTable(tableName, "id VARCHAR(10) PRIMARY KEY", "name VARCHAR(100)");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES('0', 'name-0')");
        executeJdbc("INSERT INTO " + quote(tableName) + " VALUES('1', 'name-1')");

        execute("CREATE MAPPING " + tableName
                + " DATA CONNECTION " + TEST_DATABASE_REF
        );

        // If you change LinkedHashMap -> HashMap at JdbcSqlConnector:159, it will fail.
        assertRowsAnyOrder(
                "SELECT * FROM " + tableName,
                asList(new Row("0", "name-0"), new Row("1", "name-1"))
        );
    }

    @Test
    public void when_mappingIsDeclaredWithDataConnection_then_itIsAvailable() throws Exception {
        // given
        String dcName = randomName();
        String name = randomName();
        createTable(name);
        Map<String, String> options = new HashMap<>();
        options.put("jdbcUrl", dbConnectionUrl);

        createDataConnection(instance(), dcName, "JDBC", false, options);
        InternalDataConnectionService dlService = getNodeEngineImpl(instance()).getDataConnectionService();
        assertThat(dlService.existsSqlDataConnection(dcName)).isTrue();

        createJdbcMappingUsingDataConnection(name, dcName);
        try (SqlResult mappings = sqlService.execute("SHOW MAPPINGS")) {
            Iterator<SqlRow> resultIt = mappings.iterator();
            assertThat(resultIt.hasNext()).isTrue();

            Object object = resultIt.next().getObject(0);
            assertThat(resultIt.hasNext()).isFalse();
            assertThat(object).isEqualTo(name);
        }

        // cleanup
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);
    }

    /**
     * Source : <a href=https://github.com/hazelcast/hazelcast/issues/24337">issue #24337</a>.
     */
    @Test
    public void given_mappingIsDeclaredWithDataConn_when_DataConnWasRemoved_then_success() throws Exception {
        // given
        String dcName = randomName();
        String mappingName = randomName();
        createTable(mappingName);
        Map<String, String> options = new HashMap<>();
        options.put("jdbcUrl", dbConnectionUrl);

        // when
        createDataConnection(instance(), dcName, "JDBC", false, options);
        createJdbcMappingUsingDataConnection(mappingName, dcName);
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);

        // then
        List<Row> showDataConnections = allRows("SHOW DATA CONNECTIONS ", sqlService);
        Row expectedConnection = new Row(TEST_DATABASE_REF, "jdbc", jsonArray("Table"));
        assertThat(showDataConnections).contains(expectedConnection);

        // Ensure that engine is not broken after Data Connection removal with some unrelated query.
        List<Row> showMappings = allRows("SHOW MAPPINGS ", sqlService);
        assertThat(showMappings).contains(new Row(mappingName));

        // Mapping shouldn't provide data, since data connection was removed.
        assertThatThrownBy(() -> sqlService.execute("SELECT * FROM " + mappingName))
                .hasMessageContaining("com.hazelcast.core.HazelcastException: Data connection '"
                        + dcName + "' not found");

        // cleanup
        sqlService.executeUpdate("DROP MAPPING " + mappingName);
    }

    @Test
    public void when_dataConnectionIsAltered_then_mappingsConnectorTypesAreUpdated() throws Exception {
        // given
        String dcName = randomName();
        String mappingName = randomName();
        createTable(mappingName);

        // when
        createDataConnection(instance(), dcName, "JDBC", false, singletonMap("jdbcUrl", dbConnectionUrl));
        createJdbcMappingUsingDataConnection(mappingName, dcName);
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);
        createDataConnection(instance(), dcName, "Kafka", false, singletonMap("A", "B"));

        // then
        assertRowsAnyOrder("SELECT * FROM information_schema.mappings ", singletonList(
                new Row(
                        "hazelcast",
                        "public",
                        mappingName,
                        '"' + mappingName + '"',
                        "kafka",
                        "{}")));

        // cleanup
        sqlService.executeUpdate("DROP MAPPING " + mappingName);
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);
    }

    @Test
    public void when_dataConnectionIsDropped_then_cachedPlansAreInvalidated() throws Exception {
        // given
        String dcName = randomName();
        String mappingName = randomName();
        createTable(mappingName);

        createDataConnection(instance(), dcName, "JDBC", false, singletonMap("jdbcUrl", dbConnectionUrl));
        createJdbcMappingUsingDataConnection(mappingName, dcName);
        // cache plan
        assertRowsAnyOrder("select count(*) from " + mappingName, new Row(0L));
        assertThat(sqlServiceImpl(instance()).getPlanCache().size()).isOne();

        // when
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);

        // then
        assertThat(sqlServiceImpl(instance()).getPlanCache().size()).as("Plan should be invalidated").isZero();
        assertThatThrownBy(() -> sqlService.execute("select count(*) from " + mappingName).iterator().hasNext())
                .as("Cached plan is not used")
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Mapping '" + mappingName + "' is invalid");

        // cleanup
        sqlService.executeUpdate("DROP MAPPING " + mappingName);
    }

    @Test
    public void when_dataConnectionIsAlteredToCorrect_then_usesNewConnectionType() throws Exception {
        // given
        String dcName = randomName();
        String mappingName = randomName();
        createTable(mappingName);

        createDataConnection(instance(), dcName, "JDBC", false, singletonMap("jdbcUrl", dbConnectionUrl));
        createJdbcMappingUsingDataConnection(mappingName, dcName);
        // cache plan with JDBC
        assertRowsAnyOrder("select count(*) from " + mappingName, new Row(0L));

        // when
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);
        createDataConnection(instance(), dcName, "Kafka", false, singletonMap("A", "B"));

        // then
        assertThatThrownBy(() -> sqlService.execute("select count(*) from " + mappingName).iterator().hasNext())
                .as("Should detect change of data connection type")
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Missing 'valueFormat' option");

        // cleanup
        sqlService.executeUpdate("DROP MAPPING " + mappingName);
        sqlService.executeUpdate("DROP DATA CONNECTION " + dcName);
    }

    @Test
    public void createMappingAutoResolveColumns() throws Exception {
        createTable(tableName);

        execute("CREATE MAPPING " + tableName
                + " DATA CONNECTION " + TEST_DATABASE_REF
        );

        try (SqlResult result = sqlService.execute("SELECT * FROM " + tableName)) {
            SqlRowMetadata metadata = result.getRowMetadata();

            assertThat(metadata.getColumns())
                    .extracting(SqlColumnMetadata::getName)
                    .contains("id", "name");
        }
    }

    // TEXT is not a standard SQL column type. This test may not run on all DBs
    @Test
    public void createMappingWithTextColumnType() throws Exception {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        MySQLDatabaseProvider.class,
                        PostgresDatabaseProvider.class,
                        H2DatabaseProvider.class
                );

        executeJdbc("CREATE TABLE " + tableName + " (id INTEGER NOT NULL,name TEXT NOT NULL)");

        insertItems(tableName, 1);

        assertThatCode(() ->
                execute("CREATE MAPPING myMapping "
                        + " EXTERNAL NAME " + tableName
                        + " ("
                        + "id INTEGER,"
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + TEST_DATABASE_REF
                )
        ).doesNotThrowAnyException();

        assertRowsAnyOrder(
                "SELECT * FROM myMapping",
                singletonList(new Row(0, "name-0"))
        );
    }

    // Postgres + MySQL : Test that table in another DB exists
    @Test
    public void createMappingFails_tableExistInAnotherDatabase_externalNameOnlyTableName() throws SQLException {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        MySQLDatabaseProvider.class,
                        PostgresDatabaseProvider.class
                );
        HazelcastInstance instance = instance();
        Config config = instance.getConfig();

        // Create table on first DB
        createTable(tableName);

        String newDBName = "db1";
        executeJdbc("CREATE DATABASE " + newDBName);

        // Add a new DB
        String newDbUrl = dbConnectionUrl.replace("com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport",
                newDBName);

        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", newDbUrl);

        String NEW_TEST_DATABASE_REF = "testDatabaseRef1";

        config.addDataConnectionConfig(
                new DataConnectionConfig(NEW_TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );

        // Create mapping to new DB. Table does not exist on new DB, and we should get an exception
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " EXTERNAL NAME " + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + NEW_TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class)
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not execute readDbFields for table");
    }

    // Postgres : Test that table in another DB and explicit schema name exists
    @Test
    public void createMappingFails_tableExistInAnotherDatabase_externalNameFullName() throws SQLException {
        assumeThat(databaseProvider)
                .isInstanceOfAny(
                        PostgresDatabaseProvider.class
                );
        HazelcastInstance instance = instance();
        Config config = instance.getConfig();

        // Create table on first DB
        createTable(tableName);

        String newDBName = "db2";
        executeJdbc("CREATE DATABASE " + newDBName);

        // Add a new DB
        String newDbUrl = dbConnectionUrl.replace("com.hazelcast.jet.sql.impl.connector.jdbc.JdbcSqlTestSupport",
                newDBName);

        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", newDbUrl);

        String NEW_TEST_DATABASE_REF = "testDatabaseRef2";

        config.addDataConnectionConfig(
                new DataConnectionConfig(NEW_TEST_DATABASE_REF)
                        .setType("jdbc")
                        .setProperties(properties)
        );

        // Create mapping to new DB. Table does not exist on new DB, and we should get an exception
        assertThatThrownBy(() -> execute(
                "CREATE MAPPING " + tableName + " EXTERNAL NAME " + newDBName + ".public." + tableName + " ("
                        + " id INT, "
                        + " name VARCHAR "
                        + ") "
                        + "DATA CONNECTION " + NEW_TEST_DATABASE_REF
        ))
                .isInstanceOf(HazelcastSqlException.class)
                .hasMessageContaining("Could not execute readDbFields for table");
    }
}
